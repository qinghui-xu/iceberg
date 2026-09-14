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

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

abstract class SparkDataFileRewriteRunner
    extends SparkRewriteRunner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {

  /**
   * Max amount of applicable equality-delete records the reader-local delete filter should handle
   * for one scan task. When a scan task of a file group references equality delete files with
   * record counts equal to or above this threshold, the group will be handled with the merge/join
   * path: instead of being loaded into executor memory, equality deletes are merged per equality
   * field-id set into Spark DataFrames and applied to data with sequence-aware joins.
   *
   * <p>A negative value disables the merge/join path. Zero means using merge/join for every file
   * group that has at least one equality delete file (position deletes and deletion vectors are not
   * accounted).
   */
  public static final String EQ_DELETE_JOIN_THRESHOLD_RECORDS = "eq-delete-join-threshold-records";

  /** Default value is to disable the merge/join path. */
  public static final long EQ_DELETE_JOIN_THRESHOLD_RECORDS_DEFAULT = -1L;

  /**
   * In order to reuse eq deletes across file groups (eg. within the same partition), merged eq
   * delete dataframe are cached. This option is used to tune the eq delete DF persistence strategy.
   */
  public static final String EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL =
      "eq-delete-join-cache-storage-level";

  /** Default eq delete DF persistence level. */
  public static final String EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL_DEFAULT = "MEMORY_AND_DISK";

  private final SparkTableCache tableCache = SparkTableCache.get();
  private final ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
  private final FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();

  private long eqDeleteJoinThresholdRecords = EQ_DELETE_JOIN_THRESHOLD_RECORDS_DEFAULT;
  private StorageLevel eqDeleteJoinCacheStorageLevel =
      StorageLevel.fromString(EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL_DEFAULT);

  SparkDataFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  abstract void doRewrite(String groupId, RewriteFileGroup fileGroup);

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of(EQ_DELETE_JOIN_THRESHOLD_RECORDS, EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL);
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.eqDeleteJoinThresholdRecords =
        PropertyUtil.propertyAsLong(
            options, EQ_DELETE_JOIN_THRESHOLD_RECORDS, EQ_DELETE_JOIN_THRESHOLD_RECORDS_DEFAULT);
    this.eqDeleteJoinCacheStorageLevel = cacheStorageLevel(options);
  }

  long eqDeleteJoinThresholdRecords() {
    return eqDeleteJoinThresholdRecords;
  }

  StorageLevel eqDeleteJoinCacheStorageLevel() {
    return eqDeleteJoinCacheStorageLevel;
  }

  @Override
  public Set<DataFile> rewrite(RewriteFileGroup group) {
    String groupId = UUID.randomUUID().toString();
    try {
      tableCache.add(groupId, table());
      taskSetManager.stageTasks(table(), groupId, group.fileScanTasks());

      doRewrite(groupId, group);

      return coordinator.fetchNewFiles(table(), groupId);
    } finally {
      tableCache.remove(groupId);
      taskSetManager.removeTasks(table(), groupId);
      coordinator.clearRewrite(table(), groupId);
    }
  }

  private static StorageLevel cacheStorageLevel(Map<String, String> options) {
    String storageLevelName =
        PropertyUtil.propertyAsString(
            options,
            EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL,
            EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL_DEFAULT);
    try {
      return StorageLevel.fromString(storageLevelName.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Cannot parse '%s' value %s as a Spark storage level",
              EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL,
              storageLevelName),
          e);
    }
  }
}
