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

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.max;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Builds the Spark scans used by the equality-delete merge/join path.
 *
 * <p>Data rows are read through the regular staged Iceberg scan with equality deletes removed from
 * the scan tasks. Equality delete files are read through the same staged scan by presenting each
 * delete file as a data file. Key columns are {@link EqualityKeyPath} expressions resolved by field
 * ID, so keys nested in structs are compared as the reader-local path compares them. Both reads
 * identify the source file of every row in {@link #FILE_COLUMN}, which a broadcast join uses to
 * attach the data sequence number and the partition scope of that file.
 *
 * <p>The file location comes from {@code input_file_name()}, not from the {@code _file} metadata
 * column: a staged rewrite read of a row-lineage table already carries {@code _row_id} and {@code
 * _last_updated_sequence_number} in its output marked as metadata columns, and Spark then exposes
 * only those as the relation's metadata output, so {@code _file} cannot be resolved at all. The
 * Iceberg readers set the value from {@code task.file().location()}, which is exactly what {@link
 * #fileAttributes} keys on, and the projection that reads it sits directly above the scan.
 */
class EqualityDeleteScans {
  static final String FILE_COLUMN = "__rewrite_file_path";
  static final String LOCATION_COLUMN = "__rewrite_file_location";
  static final String SEQUENCE_NUMBER_COLUMN = "__rewrite_data_sequence_number";
  static final String SCOPE_COLUMN = "__rewrite_scope_id";
  static final String DELETE_KEY_PREFIX = "__rewrite_delete_key_";
  static final String DELETE_SCOPE_COLUMN = "__rewrite_delete_scope_id";
  static final String DELETE_SEQUENCE_NUMBER_COLUMN = "__rewrite_delete_sequence_number";

  private static final StructType FILE_ATTRIBUTES_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField(LOCATION_COLUMN, DataTypes.StringType, false, Metadata.empty()),
            new StructField(SEQUENCE_NUMBER_COLUMN, DataTypes.LongType, false, Metadata.empty()),
            new StructField(SCOPE_COLUMN, DataTypes.IntegerType, false, Metadata.empty())
          });

  private final SparkSession spark;
  private final Table table;
  private final EqualityDeleteJoinPlan plan;

  EqualityDeleteScans(SparkSession spark, Table table, EqualityDeleteJoinPlan plan) {
    Preconditions.checkArgument(plan != null, "Invalid equality-delete join plan: null");
    this.spark = spark;
    this.table = table;
    this.plan = plan;
  }

  /**
   * Copies a planned whole-file task keeping position deletes and deletion vectors but dropping
   * equality deletes, which the join path applies itself.
   */
  static FileScanTask withoutEqualityDeletes(FileScanTask task) {
    Preconditions.checkArgument(
        task.start() == 0 && task.length() == task.file().fileSizeInBytes(),
        "Cannot remove equality deletes from a split scan task: %s",
        task);
    DeleteFile[] remainingDeletes =
        task.deletes().stream()
            .filter(delete -> delete.content() != FileContent.EQUALITY_DELETES)
            .toArray(DeleteFile[]::new);
    return new BaseFileScanTask(
        task.file(),
        remainingDeletes,
        SchemaParser.toJson(task.schema()),
        PartitionSpecParser.toJson(task.spec()),
        ResidualEvaluator.unpartitioned(task.residual()));
  }

  /** Presents an equality delete file as a data file so the staged scan can read its rows. */
  static FileScanTask asDataScanTask(Table table, DeleteFile deleteFile) {
    PartitionSpec spec = table.specs().get(deleteFile.specId());
    Preconditions.checkArgument(
        spec != null,
        "Cannot find partition spec %s for delete file %s",
        deleteFile.specId(),
        deleteFile.location());
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath(deleteFile.location())
            .withFormat(deleteFile.format())
            .withPartition(deleteFile.partition())
            .withRecordCount(deleteFile.recordCount())
            .withFileSizeInBytes(deleteFile.fileSizeInBytes())
            .withSplitOffsets(deleteFile.splitOffsets())
            .withEncryptionKeyMetadata(deleteFile.keyMetadata())
            .build();
    return new BaseFileScanTask(
        dataFile,
        null,
        SchemaParser.toJson(table.schema()),
        PartitionSpecParser.toJson(spec),
        ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));
  }

  /** Column paths of the equality fields in the current table schema, in the given order. */
  List<EqualityKeyPath> keyPaths(List<Integer> equalityFieldIds) {
    return EqualityKeyPath.of(table.schema(), equalityFieldIds);
  }

  /**
   * One row per file with its location, data sequence number and partition scope ID.
   *
   * <p>The scope IDs come from the plan so that both sides of a partition-scoped join agree on
   * them, and so that partitions are compared structurally rather than by their human-readable
   * path, which renders a string value of {@code "null"} and an actual {@code NULL} the same way.
   *
   * <p>The dataset will be coalesced to 1 partition. If we let Spark decide the number of
   * partitions which will be min(num_files, total_executor_cores), it can be large and creating a
   * lot of tasks, leading to high Spark scheduling delay and eventually a timeout when broadcasting
   * it to join with data. In fact, as the collection is pretty small (as it fits into driver
   * memory), one single task should be enough to compute the dataset.
   */
  Dataset<Row> fileAttributes(Iterable<? extends ContentFile<?>> files) {
    List<Row> rows = Lists.newArrayList();
    for (ContentFile<?> file : files) {
      rows.add(RowFactory.create(file.location(), file.dataSequenceNumber(), plan.scopeId(file)));
    }

    return spark.createDataFrame(rows, FILE_ATTRIBUTES_SCHEMA).coalesce(1);
  }

  /**
   * Reads every delete file of the key once and merges the rows by equality key (and partition
   * scope for partition-scoped keys), keeping the latest delete sequence number per key.
   *
   * <p>Each delete row is tagged with the delete file it came from through {@code
   * input_file_name()}, which the Iceberg readers set from {@code task.file().location()} and which
   * is evaluated in the projection directly above the scan. The {@code _file} metadata column
   * cannot be used here because a staged rewrite read of a row-lineage table exposes only its
   * lineage columns as metadata output.
   *
   * <p>The delete file scan tasks are staged under {@code stagingId}; the caller must remove that
   * ID from {@link SparkTableCache} and {@link ScanTaskSetManager} once the DataFrame is released.
   */
  Dataset<Row> mergedDeletes(DeleteCacheKey key, String stagingId) {
    List<EqualityKeyPath> keyPaths = keyPaths(key.equalityFieldIds());
    List<FileScanTask> tasks =
        key.deleteFiles().stream()
            .map(deleteFile -> asDataScanTask(table, deleteFile))
            .collect(Collectors.toList());

    SparkTableCache.get().add(stagingId, table);
    ScanTaskSetManager.get().stageTasks(table, stagingId, tasks);

    Dataset<Row> staged =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, stagingId)
            .load(stagingId);

    List<Column> projection = Lists.newArrayList();
    for (int pos = 0; pos < keyPaths.size(); pos++) {
      projection.add(keyPaths.get(pos).column(staged::col).as(DELETE_KEY_PREFIX + pos));
    }
    projection.add(input_file_name().as(FILE_COLUMN));

    Dataset<Row> deleteRows = staged.select(projection.toArray(new Column[0]));

    Dataset<Row> attributes = fileAttributes(key.deleteFiles());
    Dataset<Row> tagged =
        deleteRows.join(
            broadcast(attributes),
            deleteRows.col(FILE_COLUMN).equalTo(attributes.col(LOCATION_COLUMN)),
            "inner");

    List<Column> groupingColumns = Lists.newArrayList();
    for (int pos = 0; pos < keyPaths.size(); pos++) {
      groupingColumns.add(col(DELETE_KEY_PREFIX + pos));
    }
    if (key.partitionScoped()) {
      groupingColumns.add(col(SCOPE_COLUMN).as(DELETE_SCOPE_COLUMN));
    }

    return tagged
        .groupBy(groupingColumns.toArray(new Column[0]))
        .agg(max(col(SEQUENCE_NUMBER_COLUMN)).as(DELETE_SEQUENCE_NUMBER_COLUMN));
  }
}
