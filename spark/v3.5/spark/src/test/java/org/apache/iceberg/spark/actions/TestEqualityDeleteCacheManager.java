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

import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;

public class TestEqualityDeleteCacheManager extends TestBase {

  private static final DeleteCacheKey KEY_A = key("a");
  private static final DeleteCacheKey KEY_B = key("b");
  private static final DeleteCacheKey KEY_C = key("c");

  /** Records builds and unstage calls so tests can assert exactly-once behavior. */
  private static class RecordingBuilder
      implements BiFunction<DeleteCacheKey, String, Dataset<Row>> {
    private final Map<DeleteCacheKey, AtomicInteger> builds = Maps.newConcurrentMap();
    private final Map<DeleteCacheKey, Dataset<Row>> dataFrames = Maps.newConcurrentMap();
    private final Map<DeleteCacheKey, String> stagingIds = Maps.newConcurrentMap();
    private final List<String> unstaged = Lists.newCopyOnWriteArrayList();

    @Override
    public Dataset<Row> apply(DeleteCacheKey key, String stagingId) {
      builds.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
      stagingIds.put(key, stagingId);
      // Spark caches by logical plan, so every key must produce a distinct plan
      Dataset<Row> df = spark.range(3).toDF().withColumn("cache_key", lit(key.locations().get(0)));
      dataFrames.put(key, df);
      return df;
    }

    int builds(DeleteCacheKey key) {
      return builds.getOrDefault(key, new AtomicInteger()).get();
    }

    boolean unstaged(DeleteCacheKey key) {
      return unstaged.contains(stagingIds.get(key));
    }

    long unstageCount(DeleteCacheKey key) {
      return unstaged.stream().filter(id -> id.equals(stagingIds.get(key))).count();
    }
  }

  private static EqualityDeleteCacheManager manager(
      RecordingBuilder builder, Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup) {
    return new EqualityDeleteCacheManager(
        requiredCachesByGroup, StorageLevel.MEMORY_AND_DISK(), builder, builder.unstaged::add);
  }

  @Test
  public void sharedCachesArePersistedAndReleasedAfterTheirOwnLastConsumer() {
    RecordingBuilder builder = new RecordingBuilder();
    Map<Integer, Set<DeleteCacheKey>> required =
        ImmutableMap.of(
            1, ImmutableSet.of(KEY_A, KEY_B),
            2, ImmutableSet.of(KEY_A, KEY_C),
            3, ImmutableSet.of(KEY_C));

    try (EqualityDeleteCacheManager manager = manager(builder, required)) {
      assertThat(manager.size()).isEqualTo(3);
      assertThat(manager.remainingConsumers(KEY_A)).containsExactlyInAnyOrder(1, 2);
      assertThat(manager.remainingConsumers(KEY_B)).containsExactly(1);
      assertThat(manager.remainingConsumers(KEY_C)).containsExactlyInAnyOrder(2, 3);

      Dataset<Row> cacheA = manager.mergedDeletes(KEY_A);
      Dataset<Row> cacheB = manager.mergedDeletes(KEY_B);
      assertThat(manager.mergedDeletes(KEY_A)).isSameAs(cacheA);
      assertThat(builder.builds(KEY_A)).as("Shared cache is built once").isEqualTo(1);
      assertThat(manager.isPersisted(KEY_A)).as("Two consumers: persisted").isTrue();
      assertThat(cacheA.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());
      assertThat(manager.isPersisted(KEY_B)).as("Single consumer: not persisted").isFalse();
      assertThat(cacheB.storageLevel()).isEqualTo(StorageLevel.NONE());

      manager.onGroupTerminal(1);
      assertThat(manager.size()).as("B released, A and C remain").isEqualTo(2);
      assertThat(builder.unstaged(KEY_B)).isTrue();
      assertThat(builder.unstaged(KEY_A)).isFalse();
      assertThat(manager.remainingConsumers(KEY_A)).containsExactly(2);
      assertThat(cacheA.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());

      manager.mergedDeletes(KEY_C);
      manager.onGroupTerminal(2);
      assertThat(manager.size()).as("A released, C remains for group 3").isEqualTo(1);
      assertThat(builder.unstaged(KEY_A)).isTrue();
      assertThat(cacheA.storageLevel()).isEqualTo(StorageLevel.NONE());
      assertThat(manager.remainingConsumers(KEY_C)).containsExactly(3);

      manager.onGroupTerminal(3);
      assertThat(manager.size()).isZero();
      assertThat(builder.unstaged(KEY_C)).isTrue();
    }
  }

  @Test
  public void terminalNotificationIsIdempotent() {
    RecordingBuilder builder = new RecordingBuilder();
    try (EqualityDeleteCacheManager manager =
        manager(builder, ImmutableMap.of(1, ImmutableSet.of(KEY_A), 2, ImmutableSet.of(KEY_A)))) {
      manager.mergedDeletes(KEY_A);

      manager.onGroupTerminal(1);
      manager.onGroupTerminal(1);
      assertThat(manager.remainingConsumers(KEY_A))
          .as("Repeated notification must not release group 2's reference")
          .containsExactly(2);

      manager.onGroupTerminal(2);
      manager.onGroupTerminal(2);
      assertThat(manager.size()).isZero();
      assertThat(builder.unstageCount(KEY_A)).isEqualTo(1);
    }
  }

  @Test
  public void unknownGroupIsIgnored() {
    RecordingBuilder builder = new RecordingBuilder();
    try (EqualityDeleteCacheManager manager =
        manager(builder, ImmutableMap.of(1, ImmutableSet.of(KEY_A)))) {
      manager.onGroupTerminal(99);
      assertThat(manager.remainingConsumers(KEY_A)).containsExactly(1);
    }
  }

  @Test
  public void concurrentTerminalNotificationsReleaseExactlyOnce() throws Exception {
    RecordingBuilder builder = new RecordingBuilder();
    int consumers = 16;
    Map<Integer, Set<DeleteCacheKey>> required = Maps.newHashMap();
    for (int group = 1; group <= consumers; group++) {
      required.put(group, ImmutableSet.of(KEY_A));
    }

    ExecutorService executor = Executors.newFixedThreadPool(consumers);
    try (EqualityDeleteCacheManager manager = manager(builder, required)) {
      Dataset<Row> cache = manager.mergedDeletes(KEY_A);
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> futures = Lists.newArrayList();
      for (int group = 1; group <= consumers; group++) {
        int groupIndex = group;
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  manager.onGroupTerminal(groupIndex);
                  manager.onGroupTerminal(groupIndex);
                  return null;
                }));
      }

      start.countDown();
      for (Future<?> future : futures) {
        future.get(1, TimeUnit.MINUTES);
      }

      assertThat(manager.size()).isZero();
      assertThat(builder.unstageCount(KEY_A)).isEqualTo(1);
      assertThat(cache.storageLevel()).isEqualTo(StorageLevel.NONE());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void cacheCannotBeBuiltAfterItsLastConsumerReleased() {
    RecordingBuilder builder = new RecordingBuilder();
    try (EqualityDeleteCacheManager manager =
        manager(builder, ImmutableMap.of(1, ImmutableSet.of(KEY_A)))) {
      manager.onGroupTerminal(1);

      assertThatThrownBy(() -> manager.mergedDeletes(KEY_A))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("no remaining consumers");
      assertThat(builder.builds(KEY_A)).isZero();
    }
  }

  @Test
  public void unbuiltCachesAreReleasedWithoutBuildingOrUnstaging() {
    RecordingBuilder builder = new RecordingBuilder();
    try (EqualityDeleteCacheManager manager =
        manager(builder, ImmutableMap.of(1, ImmutableSet.of(KEY_A, KEY_B)))) {
      manager.onGroupTerminal(1);

      assertThat(manager.size()).isZero();
      assertThat(builder.builds(KEY_A)).isZero();
      assertThat(builder.unstaged).isEmpty();
    }
  }

  @Test
  public void closeReleasesEverythingStillPresent() {
    RecordingBuilder builder = new RecordingBuilder();
    EqualityDeleteCacheManager manager =
        manager(
            builder, ImmutableMap.of(1, ImmutableSet.of(KEY_A), 2, ImmutableSet.of(KEY_A, KEY_B)));
    Dataset<Row> cacheA = manager.mergedDeletes(KEY_A);
    manager.onGroupTerminal(1);

    manager.close();

    assertThat(manager.size()).isZero();
    assertThat(cacheA.storageLevel()).isEqualTo(StorageLevel.NONE());
    assertThat(builder.unstageCount(KEY_A)).isEqualTo(1);
    assertThat(builder.unstaged(KEY_B)).as("Never built, nothing staged").isFalse();
    assertThatThrownBy(() -> manager.mergedDeletes(KEY_A))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no remaining consumers");
  }

  @Test
  public void failedBuildStillUnstagesOnRelease() {
    RecordingBuilder builder =
        new RecordingBuilder() {
          @Override
          public Dataset<Row> apply(DeleteCacheKey key, String stagingId) {
            super.apply(key, stagingId);
            throw new RuntimeException("boom");
          }
        };
    try (EqualityDeleteCacheManager manager =
        manager(builder, ImmutableMap.of(1, ImmutableSet.of(KEY_A)))) {
      assertThatThrownBy(() -> manager.mergedDeletes(KEY_A)).hasMessage("boom");
      manager.onGroupTerminal(1);
      assertThat(builder.unstageCount(KEY_A)).isEqualTo(1);
    }
  }

  private static DeleteCacheKey key(String name) {
    DeleteFile deleteFile = mock(DeleteFile.class);
    when(deleteFile.location()).thenReturn("file:///deletes/" + name + ".parquet");
    return DeleteCacheKey.of(ImmutableList.of(1), ImmutableList.of(deleteFile), false);
  }
}
