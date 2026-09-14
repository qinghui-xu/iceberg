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

import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_KEY_PREFIX;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_SCOPE_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_SEQUENCE_NUMBER_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.LOCATION_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.SCOPE_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.SEQUENCE_NUMBER_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.LOCATION_TYPE;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.NESTED_SCHEMA;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.SCHEMA;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addEqualityDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addPositionDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.appendRows;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.cityDelete;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.fieldId;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.partition;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.record;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.struct;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.tasksByLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestEqualityDeleteScans extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Map<String, String> V2 =
      ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");

  @TempDir private File tableDir;

  @Test
  public void withoutEqualityDeletesKeepsPositionDeletesAndTaskMetadata() {
    Table table = unpartitionedTable();
    DataFile dataFile =
        appendRows(table, null, record(SCHEMA, 1, "a", "x"), record(SCHEMA, 2, "b", "x"));
    addPositionDeletes(table, null, dataFile, 0L);
    addEqualityDeletes(table, null, "id", 2);

    FileScanTask task = tasksByLocation(table).get(dataFile.location());
    assertThat(task.deletes()).hasSize(2);

    FileScanTask stripped = EqualityDeleteScans.withoutEqualityDeletes(task);

    assertThat(stripped.deletes()).hasSize(1);
    assertThat(stripped.deletes().get(0).content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(stripped.file().location()).isEqualTo(task.file().location());
    assertThat(stripped.spec()).isEqualTo(task.spec());
    assertThat(stripped.start()).isEqualTo(task.start());
    assertThat(stripped.length()).isEqualTo(task.length());
    assertThat(stripped.residual()).isEqualTo(task.residual());
    assertThat(stripped.schema().asStruct()).isEqualTo(task.schema().asStruct());
  }

  @Test
  public void asDataScanTaskCopiesDeleteFileMetadata() {
    Table table = partitionedTable();
    appendRows(table, partition("a"), record(SCHEMA, 1, "x", "a"));
    DeleteFile written = addEqualityDeletes(table, partition("a"), "id", 1);
    DeleteFile deleteFile = TestHelpers.deleteFiles(table).iterator().next();

    FileScanTask task = EqualityDeleteScans.asDataScanTask(table, deleteFile);

    assertThat(task.deletes()).isEmpty();
    assertThat(task.file().location()).isEqualTo(written.location());
    assertThat(task.file().format()).isEqualTo(written.format());
    assertThat(task.file().recordCount()).isEqualTo(written.recordCount());
    assertThat(task.file().fileSizeInBytes()).isEqualTo(written.fileSizeInBytes());
    assertThat(task.file().specId()).isEqualTo(written.specId());
    assertThat(task.file().partition().get(0, String.class)).isEqualTo("a");
    assertThat(task.spec().specId()).isEqualTo(written.specId());
    assertThat(task.start()).isEqualTo(0L);
    assertThat(task.length()).isEqualTo(written.fileSizeInBytes());
  }

  @Test
  public void keyPathsResolveFieldsByIdInRequestedOrder() {
    Table table = TABLES.create(NESTED_SCHEMA, PartitionSpec.unpartitioned(), V2, newLocation());
    EqualityDeleteScans scans = new EqualityDeleteScans(spark, table);

    List<EqualityKeyPath> paths =
        scans.keyPaths(ImmutableList.of(fieldId(table, "location.city"), fieldId(table, "id")));

    assertThat(paths).extracting(EqualityKeyPath::toString).containsExactly("location.city", "id");
    assertThatThrownBy(() -> scans.keyPaths(ImmutableList.of(99)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("equality field 99 is not in the table schema");
  }

  @Test
  public void mergedDeletesOfNestedKeysAreStructsMirroringTheProjection() {
    Table table = TABLES.create(NESTED_SCHEMA, PartitionSpec.unpartitioned(), V2, newLocation());
    appendRows(
        table, null, record(NESTED_SCHEMA, 1, struct(LOCATION_TYPE, "paris", 75000), "a")); // seq 1
    Schema deleteRowSchema = table.schema().select("location.city");
    int[] cityIds = new int[] {fieldId(table, "location.city")};
    // seq 2: paris, and a present location with a null city
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        cityIds,
        cityDelete(deleteRowSchema, "paris"),
        cityDelete(deleteRowSchema, null));
    // seq 3: paris again, and a delete whose whole location struct is null
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        cityIds,
        cityDelete(deleteRowSchema, "paris"),
        record(deleteRowSchema, (Object) null));
    DeleteCacheKey key =
        DeleteCacheKey.of(ImmutableList.of(cityIds[0]), TestHelpers.deleteFiles(table), false);

    String stagingId = UUID.randomUUID().toString();
    try {
      Dataset<Row> merged = new EqualityDeleteScans(spark, table).mergedDeletes(key, stagingId);

      assertThat(merged.columns())
          .containsExactly(DELETE_KEY_PREFIX + "0", DELETE_SEQUENCE_NUMBER_COLUMN);
      assertThat(merged.schema().apply(DELETE_KEY_PREFIX + "0").dataType())
          .isEqualTo(new StructType().add("city", DataTypes.StringType));
      assertEquals(
          "Keys are the projected location struct: null struct, struct with a null city, paris",
          ImmutableList.of(row(null, 3L), row(row((Object) null), 2L), row(row("paris"), 3L)),
          rowsToJava(merged.sort(DELETE_KEY_PREFIX + "0").collectAsList()));
    } finally {
      SparkTableCache.get().remove(stagingId);
      ScanTaskSetManager.get().removeTasks(table, stagingId);
    }
  }

  @Test
  public void fileAttributesCarrySequenceNumberAndScope() {
    Table table = partitionedTable();
    DataFile fileA = appendRows(table, partition("a"), record(SCHEMA, 1, "x", "a")); // seq 1
    DataFile fileB = appendRows(table, partition("b"), record(SCHEMA, 2, "x", "b")); // seq 2
    Map<String, FileScanTask> tasks = tasksByLocation(table);

    Dataset<Row> attributes =
        new EqualityDeleteScans(spark, table)
            .fileAttributes(
                ImmutableList.of(
                    tasks.get(fileA.location()).file(), tasks.get(fileB.location()).file()));

    assertThat(attributes.columns())
        .containsExactly(LOCATION_COLUMN, SEQUENCE_NUMBER_COLUMN, SCOPE_COLUMN);
    List<Object[]> rows = rowsToJava(attributes.sort(SEQUENCE_NUMBER_COLUMN).collectAsList());
    assertEquals(
        "Attributes must match planned files",
        ImmutableList.of(
            row(fileA.location(), 1L, "0/category=a"), row(fileB.location(), 2L, "0/category=b")),
        rows);
  }

  @Test
  public void mergedDeletesKeepLatestSequenceNumberPerKey() {
    Table table = unpartitionedTable();
    appendRows(table, null, record(SCHEMA, 1, "a", "x"), record(SCHEMA, 2, "b", "x")); // seq 1
    addEqualityDeletes(table, null, "id", 1, 2); // seq 2
    addEqualityDeletes(table, null, "id", 2, 3); // seq 3
    DeleteCacheKey key =
        DeleteCacheKey.of(ImmutableList.of(1), TestHelpers.deleteFiles(table), false);

    String stagingId = UUID.randomUUID().toString();
    try {
      Dataset<Row> merged = new EqualityDeleteScans(spark, table).mergedDeletes(key, stagingId);

      assertThat(merged.columns())
          .containsExactly(DELETE_KEY_PREFIX + "0", DELETE_SEQUENCE_NUMBER_COLUMN);
      assertEquals(
          "Each key must map to its latest delete sequence number",
          ImmutableList.of(row(1, 2L), row(2, 3L), row(3, 3L)),
          rowsToJava(merged.sort(DELETE_KEY_PREFIX + "0").collectAsList()));
    } finally {
      SparkTableCache.get().remove(stagingId);
      ScanTaskSetManager.get().removeTasks(table, stagingId);
    }
  }

  @Test
  public void mergedDeletesOfPartitionScopedFilesCarryScopeAndSupportNullKeys() {
    Table table = partitionedTable();
    appendRows(table, partition("a"), record(SCHEMA, 1, null, "a")); // seq 1
    appendRows(table, partition("b"), record(SCHEMA, 2, null, "b")); // seq 2
    addEqualityDeletes(table, partition("a"), "data", (Object) null); // seq 3
    addEqualityDeletes(table, partition("b"), "data", null, "z"); // seq 4
    DeleteCacheKey key =
        DeleteCacheKey.of(ImmutableList.of(2), TestHelpers.deleteFiles(table), true);

    String stagingId = UUID.randomUUID().toString();
    try {
      Dataset<Row> merged = new EqualityDeleteScans(spark, table).mergedDeletes(key, stagingId);

      assertThat(merged.columns())
          .containsExactly(
              DELETE_KEY_PREFIX + "0", DELETE_SCOPE_COLUMN, DELETE_SEQUENCE_NUMBER_COLUMN);
      assertEquals(
          "Null keys must be kept and scopes must come from the delete file partition",
          ImmutableList.of(
              row(null, "0/category=a", 3L),
              row(null, "0/category=b", 4L),
              row("z", "0/category=b", 4L)),
          rowsToJava(merged.sort(DELETE_SCOPE_COLUMN, DELETE_KEY_PREFIX + "0").collectAsList()));
    } finally {
      SparkTableCache.get().remove(stagingId);
      ScanTaskSetManager.get().removeTasks(table, stagingId);
    }
  }

  @Test
  public void mergedDeletesWithMultipleKeyColumnsFollowFieldIdOrder() {
    Table table = unpartitionedTable();
    appendRows(table, null, record(SCHEMA, 1, "a", "x")); // seq 1
    // equality delete keyed by (data, id): column order in the file differs from field ID order.
    // Schema#select("data", "id") would not do this: it preserves the original schema's field
    // order (id, data) regardless of the requested name order, so the schema is built directly.
    Schema deleteRowSchema =
        new Schema(table.schema().findField("data"), table.schema().findField("id"));
    Record delete = EqualityDeleteTestUtil.record(deleteRowSchema, "a", 1);
    commitEqualityDelete(table, deleteRowSchema, delete); // seq 2
    DeleteCacheKey key =
        DeleteCacheKey.of(ImmutableList.of(2, 1), TestHelpers.deleteFiles(table), false);

    String stagingId = UUID.randomUUID().toString();
    try {
      Dataset<Row> merged = new EqualityDeleteScans(spark, table).mergedDeletes(key, stagingId);

      assertThat(key.equalityFieldIds()).containsExactly(1, 2);
      assertThat(merged.columns())
          .containsExactly(
              DELETE_KEY_PREFIX + "0", DELETE_KEY_PREFIX + "1", DELETE_SEQUENCE_NUMBER_COLUMN);
      assertEquals(
          "Key columns must follow sorted field ID order",
          ImmutableList.of(row(1, "a", 2L)),
          rowsToJava(merged.collectAsList()));
    } finally {
      SparkTableCache.get().remove(stagingId);
      ScanTaskSetManager.get().removeTasks(table, stagingId);
    }
  }

  private void commitEqualityDelete(Table table, Schema deleteRowSchema, Record delete) {
    try {
      String location =
          table
              .locationProvider()
              .newDataLocation(FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
      DeleteFile deleteFile =
          FileHelpers.writeDeleteFile(
              table,
              table.io().newOutputFile(location),
              null,
              ImmutableList.of(delete),
              deleteRowSchema);
      table.newRowDelta().addDeletes(deleteFile).commit();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
