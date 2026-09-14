/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the merged equality-delete DataFrames of one rewrite action.
 *
 * <p>Every planned {@link DeleteCacheKey} gets an entry with the set of file groups (by global
 * index) that still need it. A DataFrame is built on first use, at most once; it is persisted and
 * materialized only when more than one group needs it. Each group releases its references when it
 * reaches a terminal outcome, and the DataFrame is unpersisted when the last consumer releases.
 * {@link #close()} releases anything left after abnormal termination.
 */
class EqualityDeleteCacheManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteCacheManager.class);

  private final Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup;
  private final Map<DeleteCacheKey, CacheEntry> entries;
  private final StorageLevel storageLevel;
  private final BiFunction<DeleteCacheKey, String, Dataset<Row>> builder;
  private final Consumer<String> unstage;

  EqualityDeleteCacheManager(
      SparkSession spark, Table table, EqualityDeleteJoinPlan plan, StorageLevel storageLevel) {
    this(
        plan.requiredCachesByGroup(),
        storageLevel,
        new EqualityDeleteScans(spark, table)::mergedDeletes,
        stagingId -> {
          SparkTableCache.get().remove(stagingId);
          ScanTaskSetManager.get().removeTasks(table, stagingId);
        });
  }

  @VisibleForTesting
  EqualityDeleteCacheManager(
      Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup,
      StorageLevel storageLevel,
      BiFunction<DeleteCacheKey, String, Dataset<Row>> builder,
      Consumer<String> unstage) {
    this.requiredCachesByGroup = Maps.newConcurrentMap();
    this.entries = Maps.newConcurrentMap();
    this.storageLevel = storageLevel;
    this.builder = builder;
    this.unstage = unstage;

    Map<DeleteCacheKey, Set<Integer>> consumersByKey = Maps.newLinkedHashMap();
    requiredCachesByGroup.forEach(
        (groupIndex, keys) -> {
          this.requiredCachesByGroup.put(groupIndex, ImmutableSet.copyOf(keys));
          for (DeleteCacheKey key : keys) {
            consumersByKey.computeIfAbsent(key, ignored -> Sets.newHashSet()).add(groupIndex);
          }
        });
    consumersByKey.forEach((key, consumers) -> entries.put(key, new CacheEntry(key, consumers)));
  }

  /** Merged deletes for the key, built on first use and shared by all remaining consumers. */
  Dataset<Row> mergedDeletes(DeleteCacheKey key) {
    CacheEntry entry = entries.get(key);
    Preconditions.checkState(
        entry != null, "Cannot load merged equality deletes for %s: no remaining consumers", key);
    return entry.dataFrame();
  }

  /**
   * Releases every cache reference held by the file group. Safe to call more than once; only the
   * first call for a group has an effect.
   */
  void onGroupTerminal(int groupIndex) {
    Set<DeleteCacheKey> keys = requiredCachesByGroup.remove(groupIndex);
    if (keys == null) {
      return;
    }

    for (DeleteCacheKey key : keys) {
      CacheEntry entry = entries.get(key);
      if (entry != null && entry.release(groupIndex) && entries.remove(key, entry)) {
        entry.unpersist();
      }
    }
  }

  @Override
  public void close() {
    requiredCachesByGroup.clear();
    List<DeleteCacheKey> keys = Lists.newArrayList(entries.keySet());
    for (DeleteCacheKey key : keys) {
      CacheEntry entry = entries.remove(key);
      if (entry != null) {
        LOG.warn("Releasing merged equality deletes for {} during cleanup", key);
        entry.unpersist();
      }
    }
  }

  @VisibleForTesting
  int size() {
    return entries.size();
  }

  @VisibleForTesting
  boolean isBuilt(DeleteCacheKey key) {
    CacheEntry entry = entries.get(key);
    return entry != null && entry.isBuilt();
  }

  @VisibleForTesting
  boolean isPersisted(DeleteCacheKey key) {
    CacheEntry entry = entries.get(key);
    return entry != null && entry.isBuilt() && entry.shared;
  }

  @VisibleForTesting
  Set<Integer> remainingConsumers(DeleteCacheKey key) {
    CacheEntry entry = entries.get(key);
    return entry == null ? ImmutableSet.of() : entry.remainingConsumers();
  }

  private class CacheEntry {
    private final DeleteCacheKey key;
    private final Set<Integer> remainingConsumers;
    private final boolean shared;
    private final String stagingId = UUID.randomUUID().toString();
    private Dataset<Row> dataFrame = null;
    private boolean staged = false;

    CacheEntry(DeleteCacheKey key, Set<Integer> consumers) {
      this.key = key;
      this.remainingConsumers = Sets.newHashSet(consumers);
      this.shared = consumers.size() > 1;
    }

    /** Builds the DataFrame on first call; persisted and materialized when shared. */
    synchronized Dataset<Row> dataFrame() {
      if (dataFrame == null) {
        Preconditions.checkState(
            !remainingConsumers().isEmpty(),
            "Cannot build merged equality deletes for %s: no remaining consumers",
            key);
        LOG.info(
            "Building merged equality deletes for {} ({} remaining consumers, persisted: {})",
            key,
            remainingConsumers().size(),
            shared);
        this.staged = true;
        Dataset<Row> merged = builder.apply(key, stagingId);
        if (shared) {
          merged = merged.persist(storageLevel);
          // materialize before any replacement file is written so build failures surface early
          merged.count();
        }

        this.dataFrame = merged;
      }

      return dataFrame;
    }

    synchronized boolean isBuilt() {
      return dataFrame != null;
    }

    /** Removes the group's reference; true only for the transition to no remaining consumers. */
    boolean release(int groupIndex) {
      synchronized (remainingConsumers) {
        return remainingConsumers.remove(groupIndex) && remainingConsumers.isEmpty();
      }
    }

    Set<Integer> remainingConsumers() {
      synchronized (remainingConsumers) {
        return ImmutableSet.copyOf(remainingConsumers);
      }
    }

    synchronized void unpersist() {
      if (dataFrame != null && shared) {
        dataFrame.unpersist(false);
      }

      if (staged) {
        unstage.accept(stagingId);
      }

      LOG.info("Released merged equality deletes for {}", key);
    }
  }
}
