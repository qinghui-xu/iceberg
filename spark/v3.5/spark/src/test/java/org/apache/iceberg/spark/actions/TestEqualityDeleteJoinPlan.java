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

import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.LOCATION_TYPE;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.NESTED_SCHEMA;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.SCHEMA;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addEqualityDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addPositionDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.appendRows;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.cityDelete;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.fieldId;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.group;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.partition;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.record;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.struct;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.tasksByLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.actions.EqualityDeleteJoinPlan.GroupJoinInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestEqualityDeleteJoinPlan {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Map<String, String> V2 =
      ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");

  @TempDir private File tableDir;

  // ---- threshold decision (mocked metadata: only content and record counts matter) ----

  @Test
  public void negativeThresholdDisablesMergeJoin() {
    RewriteFileGroup group = group(1, ImmutableList.of(task(eqDelete(1_000_000L))));
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group, -1L)).isFalse();
  }

  @Test
  public void zeroThresholdSelectsAnyGroupWithEqualityDeletes() {
    assertThat(
            EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task(eqDelete(0L)))), 0L))
        .isTrue();
    assertThat(
            EqualityDeleteJoinPlan.useMergeJoin(
                group(2, ImmutableList.of(task(posDelete(5L)))), 0L))
        .as("Zero selects the join path only for groups with equality deletes")
        .isFalse();
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(3, ImmutableList.of(task())), 0L))
        .isFalse();
  }

  @Test
  public void recordCountEqualToThresholdSelectsMergeJoin() {
    assertThat(
            EqualityDeleteJoinPlan.useMergeJoin(
                group(1, ImmutableList.of(task(eqDelete(100L)))), 100L))
        .isTrue();
    assertThat(
            EqualityDeleteJoinPlan.useMergeJoin(
                group(2, ImmutableList.of(task(eqDelete(99L)))), 100L))
        .isFalse();
  }

  @Test
  public void recordCountsAccumulateAcrossDeleteFilesAndFieldIdSets() {
    FileScanTask task = task(eqDelete(40L, 1), eqDelete(30L, 2), eqDelete(30L, 1, 2));
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), 100L))
        .isTrue();
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), 101L))
        .isFalse();
  }

  @Test
  public void recordCountsDoNotAccumulateAcrossTasks() {
    RewriteFileGroup group = group(1, ImmutableList.of(task(eqDelete(60L)), task(eqDelete(60L))));
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group, 100L)).isFalse();
  }

  @Test
  public void positionDeletesDoNotContributeToThreshold() {
    FileScanTask task = task(posDelete(1_000L), eqDelete(1L));
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), 2L)).isFalse();
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), 1L)).isTrue();
  }

  @Test
  public void thresholdEvaluationStopsAtFirstQualifyingTask() {
    FileScanTask second = mock(FileScanTask.class);
    RewriteFileGroup group = group(1, ImmutableList.of(task(eqDelete(10L)), second));

    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group, 10L)).isTrue();
    verify(second, never()).deletes();
  }

  @Test
  public void largeRecordCountsDoNotOverflow() {
    FileScanTask task = task(eqDelete(Long.MAX_VALUE - 1), eqDelete(Long.MAX_VALUE - 1));
    assertThat(
            EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), Long.MAX_VALUE))
        .isTrue();
  }

  // ---- plan construction (real tables so sequence numbers and specs are genuine) ----

  @Test
  public void disabledThresholdProducesEmptyPlan() {
    Table table = unpartitionedTable();
    appendRows(table, null, record(SCHEMA, 1, "a", "x"));
    addEqualityDeletes(table, null, "id", 1);

    EqualityDeleteJoinPlan plan =
        EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group(1, allTasks(table))), -1L);

    assertThat(plan.isEmpty()).isTrue();
    assertThat(plan.usesJoin(1)).isFalse();
    assertThat(plan.cacheKeys()).isEmpty();
  }

  @Test
  public void planSplitsPartitionScopedAndGlobalDeletesPerFieldIdSet() {
    Table table = partitionedTable();
    DataFile fileA = appendRows(table, partition("a"), record(SCHEMA, 1, "x", "a")); // seq 1
    DataFile fileB = appendRows(table, partition("b"), record(SCHEMA, 2, "x", "b")); // seq 2
    DeleteFile scopedDelete = addEqualityDeletes(table, partition("a"), "id", 1); // seq 3, spec 0
    table.updateSpec().removeField("category").commit(); // spec 1 is unpartitioned
    DeleteFile globalDelete = addEqualityDeletes(table, null, "data", "x"); // seq 4, global

    Map<String, FileScanTask> tasks = tasksByLocation(table);
    assertThat(tasks.get(fileA.location()).deletes()).hasSize(2);
    assertThat(tasks.get(fileB.location()).deletes()).hasSize(1);

    RewriteFileGroup group = group(7, Lists.newArrayList(tasks.values()));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    assertThat(plan.usesJoin(7)).isTrue();
    GroupJoinInfo info = plan.joinInfo(7);
    assertThat(info.dataFiles())
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(fileA.location(), fileB.location());
    assertThat(info.equalityDeleteFileCount()).isEqualTo(2);

    assertThat(info.partitionScopedKeys()).hasSize(1);
    DeleteCacheKey scopedKey = info.partitionScopedKeys().get(0);
    assertThat(scopedKey.partitionScoped()).isTrue();
    assertThat(scopedKey.equalityFieldIds()).containsExactly(1);
    assertThat(scopedKey.locations()).containsExactly(scopedDelete.location());

    assertThat(info.globalKeys()).hasSize(1);
    DeleteCacheKey globalKey = info.globalKeys().get(0);
    assertThat(globalKey.partitionScoped()).isFalse();
    assertThat(globalKey.equalityFieldIds()).containsExactly(2);
    assertThat(globalKey.locations()).containsExactly(globalDelete.location());

    assertThat(plan.requiredCachesByGroup()).containsOnlyKeys(7);
    assertThat(plan.requiredCachesByGroup().get(7)).containsExactlyInAnyOrder(scopedKey, globalKey);
    assertThat(plan.consumers(scopedKey)).containsExactly(7);
    assertThat(plan.consumers(globalKey)).containsExactly(7);
  }

  @Test
  public void groupsWithSameDeleteFilesShareOneCacheKey() {
    Table table = unpartitionedTable();
    DataFile file1 = appendRows(table, null, record(SCHEMA, 1, "a", "x"));
    DataFile file2 = appendRows(table, null, record(SCHEMA, 2, "b", "x"));
    addEqualityDeletes(table, null, "id", 1, 2);

    Map<String, FileScanTask> tasks = tasksByLocation(table);
    RewriteFileGroup group1 = group(1, ImmutableList.of(tasks.get(file1.location())));
    RewriteFileGroup group2 = group(2, ImmutableList.of(tasks.get(file2.location())));
    EqualityDeleteJoinPlan plan =
        EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group1, group2), 0L);

    assertThat(plan.cacheKeys()).hasSize(1);
    DeleteCacheKey key = plan.cacheKeys().iterator().next();
    assertThat(plan.consumers(key)).containsExactlyInAnyOrder(1, 2);
    assertThat(plan.requiredCachesByGroup().get(1)).containsExactly(key);
    assertThat(plan.requiredCachesByGroup().get(2)).containsExactly(key);
  }

  @Test
  public void groupReferencingKeyFromSeveralTasksCountsOnce() {
    Table table = unpartitionedTable();
    appendRows(table, null, record(SCHEMA, 1, "a", "x"));
    appendRows(table, null, record(SCHEMA, 2, "b", "x"));
    addEqualityDeletes(table, null, "id", 1, 2);

    RewriteFileGroup group = group(3, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    assertThat(plan.cacheKeys()).hasSize(1);
    assertThat(plan.consumers(plan.cacheKeys().iterator().next())).containsExactly(3);
    assertThat(plan.joinInfo(3).cacheKeys()).hasSize(1);
  }

  @Test
  public void differentDeleteFileSetsProduceDifferentKeys() {
    Table table = unpartitionedTable();
    DataFile file1 = appendRows(table, null, record(SCHEMA, 1, "a", "x")); // seq 1
    addEqualityDeletes(table, null, "id", 1); // seq 2, applies only to file1
    DataFile file2 = appendRows(table, null, record(SCHEMA, 2, "b", "x")); // seq 3
    // seq 4, deletes ids 1 and 2 so planning cannot prune it by bounds for either file
    addEqualityDeletes(table, null, "id", 1, 2);

    Map<String, FileScanTask> tasks = tasksByLocation(table);
    RewriteFileGroup group1 = group(1, ImmutableList.of(tasks.get(file1.location())));
    RewriteFileGroup group2 = group(2, ImmutableList.of(tasks.get(file2.location())));
    EqualityDeleteJoinPlan plan =
        EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group1, group2), 0L);

    assertThat(plan.cacheKeys()).hasSize(2);
    assertThat(plan.joinInfo(1).globalKeys().get(0).locations()).hasSize(2);
    assertThat(plan.joinInfo(2).globalKeys().get(0).locations()).hasSize(1);
  }

  @Test
  public void orderGroupsRunsReaderLocalGroupsFirstAndKeepsPlannerOrder() {
    Table table = unpartitionedTable();
    DataFile file1 = appendRows(table, null, record(SCHEMA, 1, "a", "x"));
    addEqualityDeletes(table, null, "id", 1);
    DataFile file2 = appendRows(table, null, record(SCHEMA, 2, "b", "x")); // no deletes apply
    DataFile file3 = appendRows(table, null, record(SCHEMA, 3, "c", "x"));
    addPositionDeletes(table, null, file3, 0L); // position deletes only

    Map<String, FileScanTask> tasks = tasksByLocation(table);
    RewriteFileGroup joinGroup = group(1, ImmutableList.of(tasks.get(file1.location())));
    RewriteFileGroup plainGroup = group(2, ImmutableList.of(tasks.get(file2.location())));
    RewriteFileGroup posDeleteGroup = group(3, ImmutableList.of(tasks.get(file3.location())));
    List<RewriteFileGroup> planned = ImmutableList.of(joinGroup, plainGroup, posDeleteGroup);
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, planned, 0L);

    assertThat(plan.orderGroups(planned)).containsExactly(plainGroup, posDeleteGroup, joinGroup);
    assertThat(EqualityDeleteJoinPlan.empty().orderGroups(planned))
        .containsExactly(joinGroup, plainGroup, posDeleteGroup);
  }

  @Test
  public void joinInfoForReaderLocalGroupFails() {
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.empty();
    assertThatThrownBy(() -> plan.joinInfo(42))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("File group 42 does not use the equality-delete join path");
  }

  @Test
  public void scopeKeyUsesSpecIdAndPartitionPath() {
    Table table = partitionedTable();
    assertThat(EqualityDeleteJoinPlan.scopeKey(table.spec(), partition("a")))
        .isEqualTo("0/category=a");
    Table unpartitioned = unpartitionedTable();
    assertThat(EqualityDeleteJoinPlan.scopeKey(unpartitioned.spec(), partition())).isEqualTo("0/");
  }

  @Test
  public void cacheKeyEqualityIgnoresOrderAndDuplicates() {
    DeleteFile first = eqDelete(1L, 1, 2);
    when(first.location()).thenReturn("s3://bucket/a.parquet");
    DeleteFile second = eqDelete(1L, 1, 2);
    when(second.location()).thenReturn("s3://bucket/b.parquet");

    DeleteCacheKey key1 =
        DeleteCacheKey.of(ImmutableList.of(2, 1), ImmutableList.of(second, first, first), true);
    DeleteCacheKey key2 =
        DeleteCacheKey.of(ImmutableList.of(1, 2), ImmutableList.of(first, second), true);

    assertThat(key1).isEqualTo(key2).hasSameHashCodeAs(key2);
    assertThat(key1.equalityFieldIds()).containsExactly(1, 2);
    assertThat(key1.locations()).containsExactly("s3://bucket/a.parquet", "s3://bucket/b.parquet");
    assertThat(key1.deleteFiles()).hasSize(2);
  }

  // ---- equality field validation ----

  @Test
  public void planRejectsEqualityFieldsMissingFromTheCurrentSchema() {
    Table table = unpartitionedTable();
    DataFile dataFile = mock(DataFile.class);
    when(dataFile.location()).thenReturn("file:///data/a.parquet");
    when(dataFile.dataSequenceNumber()).thenReturn(1L);
    DeleteFile delete = eqDelete(1L, 99);
    when(delete.location()).thenReturn("file:///deletes/a.parquet");
    when(delete.dataSequenceNumber()).thenReturn(2L);
    FileScanTask task = task(delete);
    when(task.file()).thenReturn(dataFile);
    RewriteFileGroup group = group(1, ImmutableList.of(task));

    assertThatThrownBy(() -> EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot use the equality-delete join path for delete file file:///deletes/a.parquet of table")
        .hasMessageContaining("equality field 99 is not in the table schema");
  }

  @Test
  public void planAcceptsEqualityFieldsNestedInStructs() {
    Table table = TABLES.create(NESTED_SCHEMA, PartitionSpec.unpartitioned(), V2, newLocation());
    appendRows(table, null, record(NESTED_SCHEMA, 1, struct(LOCATION_TYPE, "paris", 75000), "a"));
    Schema deleteRowSchema = table.schema().select("location.city");
    int cityId = fieldId(table, "location.city");
    assertThat(EqualityKeyPath.of(table.schema(), cityId).isNested()).isTrue();
    addEqualityDeletes(
        table, null, deleteRowSchema, new int[] {cityId}, cityDelete(deleteRowSchema, "paris"));
    RewriteFileGroup group = group(1, allTasks(table));

    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    assertThat(plan.usesJoin(1)).isTrue();
    assertThat(plan.joinInfo(1).globalKeys()).hasSize(1);
    assertThat(plan.joinInfo(1).globalKeys().get(0).equalityFieldIds()).containsExactly(cityId);
  }

  // ---- helpers ----

  private Table unpartitionedTable() {
    return TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), V2, newLocation());
  }

  private Table partitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();
    return TABLES.create(SCHEMA, spec, V2, newLocation());
  }

  private String newLocation() {
    return new File(tableDir, UUID.randomUUID().toString()).toURI().toString();
  }

  private static List<FileScanTask> allTasks(Table table) {
    return Lists.newArrayList(tasksByLocation(table).values());
  }

  private static FileScanTask task(DeleteFile... deletes) {
    FileScanTask task = mock(FileScanTask.class);
    when(task.deletes()).thenReturn(ImmutableList.copyOf(deletes));
    return task;
  }

  private static DeleteFile eqDelete(long recordCount, Integer... fieldIds) {
    DeleteFile delete = mock(DeleteFile.class);
    when(delete.content()).thenReturn(FileContent.EQUALITY_DELETES);
    when(delete.recordCount()).thenReturn(recordCount);
    when(delete.equalityFieldIds())
        .thenReturn(fieldIds.length == 0 ? ImmutableList.of(1) : ImmutableList.copyOf(fieldIds));
    return delete;
  }

  private static DeleteFile posDelete(long recordCount) {
    DeleteFile delete = mock(DeleteFile.class);
    when(delete.content()).thenReturn(FileContent.POSITION_DELETES);
    when(delete.recordCount()).thenReturn(recordCount);
    return delete;
  }
}
