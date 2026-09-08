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
import static org.apache.spark.sql.functions.input_file_name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
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
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestEqualityDeleteJoinFilter extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Map<String, String> V2 =
      ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");

  @TempDir private File tableDir;
  private Table stagedTable = null;
  private String stagingId = null;

  @AfterEach
  public void unstage() {
    if (stagingId != null) {
      SparkTableCache.get().remove(stagingId);
      ScanTaskSetManager.get().removeTasks(stagedTable, stagingId);
    }
  }

  @Test
  public void removesRowsOlderThanTheLatestMatchingDeleteOnly() {
    Table table = unpartitionedTable();
    appendRows(
        table,
        null,
        record(SCHEMA, 1, "a", "x"),
        record(SCHEMA, 2, "b", "x"),
        record(SCHEMA, 3, "c", "x")); // seq 1
    addEqualityDeletes(table, null, "id", 2, 3); // seq 2
    appendRows(table, null, record(SCHEMA, 3, "c2", "y")); // seq 3: newer than the delete, survives

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    EqualityDeleteScans scans = new EqualityDeleteScans(spark, table, plan);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(scans, caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertThat(survivors.columns()).containsExactly("id", "data", "category");
      assertEquals(
          "Only rows older than a matching delete are removed",
          ImmutableList.of(row(1, "a", "x"), row(3, "c2", "y")),
          rowsToJava(survivors.sort("id", "data").collectAsList()));
    }
  }

  @Test
  public void matchesNullKeysAndAppliesEveryFieldIdSet() {
    Table table = unpartitionedTable();
    appendRows(
        table,
        null,
        record(SCHEMA, 1, null, "x"),
        record(SCHEMA, 2, "b", "x"),
        record(SCHEMA, 3, "c", "x"),
        record(SCHEMA, 4, null, "x")); // seq 1
    addEqualityDeletes(table, null, "data", (Object) null); // seq 2: deletes ids 1 and 4
    addEqualityDeletes(table, null, "id", 3); // seq 3: second field-id set

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    assertThat(plan.joinInfo(1).globalKeys()).hasSize(2);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertEquals(
          "Null keys match null deletes; a row deleted by any field-id set is removed",
          ImmutableList.of(row(2, "b", "x")),
          rowsToJava(survivors.collectAsList()));
    }
  }

  @Test
  public void partitionScopedDeletesApplyOnlyToTheirPartition() {
    Table table = partitionedTable();
    appendRows(table, partition("x"), record(SCHEMA, 1, "a", "x")); // seq 1
    appendRows(table, partition("y"), record(SCHEMA, 2, "a", "y")); // seq 2
    addEqualityDeletes(table, partition("x"), "data", "a"); // seq 3, scoped to category=x

    // one group holding files of two partitions, as happens after partition evolution
    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    assertThat(plan.joinInfo(1).partitionScopedKeys()).hasSize(1);
    assertThat(plan.joinInfo(1).globalKeys()).isEmpty();

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertEquals(
          "The delete scoped to category=x must not remove the matching row in category=y",
          ImmutableList.of(row(2, "a", "y")),
          rowsToJava(survivors.collectAsList()));
    }
  }

  @Test
  public void globalDeletesApplyAcrossPartitionsWhilePositionDeletesStayWithTheReader() {
    Table table = partitionedTable();
    DataFile fileX =
        appendRows(
            table,
            partition("x"),
            record(SCHEMA, 1, "a", "x"),
            record(SCHEMA, 5, "e", "x")); // seq 1
    appendRows(table, partition("y"), record(SCHEMA, 2, "a", "y")); // seq 2
    addPositionDeletes(table, partition("x"), fileX, 1L); // seq 3: removes (5, e, x)
    table.updateSpec().removeField("category").commit();
    addEqualityDeletes(table, null, "data", "a"); // seq 4: global, removes both "a" rows
    appendRows(table, null, record(SCHEMA, 3, "a", "z")); // seq 5: newer, survives

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    assertThat(plan.joinInfo(1).partitionScopedKeys()).isEmpty();
    assertThat(plan.joinInfo(1).globalKeys()).hasSize(1);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertEquals(
          "Global deletes remove older rows in every partition; position deletes are still applied",
          ImmutableList.of(row(3, "a", "z")),
          rowsToJava(survivors.collectAsList()));
    }
  }

  @Test
  public void partitionScopedDeletesDistinguishNullFromTheStringNull() {
    Table table = partitionedTable();
    // both partitions render to the same human-readable path, category=null
    appendRows(table, partition((Object) null), record(SCHEMA, 1, "a", null)); // seq 1
    appendRows(table, partition("null"), record(SCHEMA, 2, "a", "null")); // seq 2
    addEqualityDeletes(table, partition((Object) null), "data", "a"); // seq 3, scoped to NULL

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    assertThat(plan.joinInfo(1).partitionScopedKeys()).hasSize(1);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertEquals(
          "The delete scoped to a NULL partition must not remove the row of category='null'",
          ImmutableList.of(row(2, "a", "null")),
          rowsToJava(survivors.collectAsList()));
    }
  }

  @Test
  public void nestedKeysMatchByFieldIdWithStructProjectionSemantics() {
    Table table = TABLES.create(NESTED_SCHEMA, PartitionSpec.unpartitioned(), V2, newLocation());
    appendRows(
        table,
        null,
        record(NESTED_SCHEMA, 1, struct(LOCATION_TYPE, "paris", 75000), "a"),
        record(NESTED_SCHEMA, 2, struct(LOCATION_TYPE, "rome", 100), "b"),
        record(NESTED_SCHEMA, 3, struct(LOCATION_TYPE, null, 200), "c"),
        record(NESTED_SCHEMA, 4, null, "d")); // seq 1
    Schema deleteRowSchema = table.schema().select("location.city");
    int cityId = fieldId(table, "location.city");
    // seq 2: deletes location.city = paris and present locations with a null city
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        new int[] {cityId},
        cityDelete(deleteRowSchema, "paris"),
        cityDelete(deleteRowSchema, null));
    appendRows(
        table, null, record(NESTED_SCHEMA, 5, struct(LOCATION_TYPE, "paris", 75001), "e")); // seq 3

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertThat(survivors.columns()).containsExactly("id", "location", "data");
      List<Object[]> joined = rowsToJava(survivors.sort("id").collectAsList());
      assertEquals(
          "paris and the null-city row are deleted; the null-location row and the newer paris row survive",
          ImmutableList.of(
              row(2, row("rome", 100), "b"), row(4, null, "d"), row(5, row("paris", 75001), "e")),
          joined);

      List<Object[]> readerLocal =
          rowsToJava(
              spark.read().format("iceberg").load(table.location()).sort("id").collectAsList());
      assertEquals("The join path must agree with the reader-local path", readerLocal, joined);
    }
  }

  @Test
  public void nestedKeyDeleteWithNullParentRemovesOnlyNullParentRows() {
    Table table = TABLES.create(NESTED_SCHEMA, PartitionSpec.unpartitioned(), V2, newLocation());
    appendRows(
        table,
        null,
        record(NESTED_SCHEMA, 1, struct(LOCATION_TYPE, null, 100), "a"),
        record(NESTED_SCHEMA, 2, null, "b")); // seq 1
    Schema deleteRowSchema = table.schema().select("location.city");
    int cityId = fieldId(table, "location.city");
    // seq 2: a delete row whose whole location struct is null
    addEqualityDeletes(
        table, null, deleteRowSchema, new int[] {cityId}, record(deleteRowSchema, (Object) null));

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      List<Object[]> joined = rowsToJava(survivors.collectAsList());
      assertEquals(
          "A null location does not match a present location with a null city",
          ImmutableList.of(row(1, row(null, 100), "a")),
          joined);
      List<Object[]> readerLocal =
          rowsToJava(spark.read().format("iceberg").load(table.location()).collectAsList());
      assertEquals("The join path must agree with the reader-local path", readerLocal, joined);
    }
  }

  @Test
  public void nestedAndTopLevelKeysInOneFieldIdSet() {
    Table table = TABLES.create(NESTED_SCHEMA, PartitionSpec.unpartitioned(), V2, newLocation());
    appendRows(
        table,
        null,
        record(NESTED_SCHEMA, 1, struct(LOCATION_TYPE, "paris", 75000), "a"),
        record(NESTED_SCHEMA, 1, struct(LOCATION_TYPE, "rome", 100), "b"),
        record(NESTED_SCHEMA, 2, struct(LOCATION_TYPE, "paris", 75000), "c")); // seq 1
    Schema deleteRowSchema = table.schema().select("id", "location.city"); // fields (id, location)
    Types.StructType deleteLocationType =
        deleteRowSchema.findField("location").type().asStructType();
    int idId = fieldId(table, "id");
    int cityId = fieldId(table, "location.city");
    // seq 2: keyed by (id, location.city); only (1, paris) matches
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        new int[] {idId, cityId},
        record(deleteRowSchema, 1, struct(deleteLocationType, "paris")));

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    assertThat(plan.joinInfo(1).globalKeys().get(0).equalityFieldIds())
        .containsExactly(idId, cityId);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertEquals(
          "Only the row matching both the top-level and the nested key is removed",
          ImmutableList.of(row(1, row("rome", 100), "b"), row(2, row("paris", 75000), "c")),
          rowsToJava(survivors.sort("id", "data").collectAsList()));
    }
  }

  @Test
  public void rowsFromAFileWithoutRewriteAttributesFailTheRewrite() {
    Table table = unpartitionedTable();
    DataFile planned = appendRows(table, null, record(SCHEMA, 1, "a", "x")); // seq 1
    DataFile unplanned = appendRows(table, null, record(SCHEMA, 2, "b", "x")); // seq 2
    addEqualityDeletes(table, null, "id", 1, 2); // seq 3

    Map<String, FileScanTask> tasks = tasksByLocation(table);
    // the group plans one file while the read is staged with both, as a divergence between
    // planning and reading would cause
    RewriteFileGroup group = group(1, ImmutableList.of(tasks.get(planned.location())));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table, plan), caches)
              .filter(
                  readWithoutEqualityDeletes(table, Lists.newArrayList(tasks.values())),
                  plan.joinInfo(1));

      assertThatThrownBy(survivors::collectAsList)
          .hasStackTraceContaining("Cannot find rewrite attributes for source file")
          .hasStackTraceContaining(unplanned.location());
    }
  }

  private Dataset<Row> readWithoutEqualityDeletes(Table table, RewriteFileGroup group) {
    return readWithoutEqualityDeletes(table, group.fileScanTasks());
  }

  private Dataset<Row> readWithoutEqualityDeletes(Table table, List<FileScanTask> groupTasks) {
    this.stagedTable = table;
    this.stagingId = UUID.randomUUID().toString();
    List<FileScanTask> tasks =
        groupTasks.stream()
            .map(EqualityDeleteScans::withoutEqualityDeletes)
            .collect(Collectors.toList());
    SparkTableCache.get().add(stagingId, table);
    ScanTaskSetManager.get().stageTasks(table, stagingId, tasks);
    return spark
        .read()
        .format("iceberg")
        .option(SparkReadOptions.SCAN_TASK_SET_ID, stagingId)
        .load(stagingId)
        .withColumn(EqualityDeleteScans.FILE_COLUMN, input_file_name());
  }

  private static List<FileScanTask> allTasks(Table table) {
    return Lists.newArrayList(tasksByLocation(table).values());
  }

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
}
