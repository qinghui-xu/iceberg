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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

/** Builds small tables with data, equality delete and position delete files for join-path tests. */
final class EqualityDeleteTestUtil {

  static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "category", Types.StringType.get()));

  /** Schema with a struct column whose fields can be equality keys. */
  static final Schema NESTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "location",
              Types.StructType.of(
                  optional(3, "city", Types.StringType.get()),
                  optional(4, "zip", Types.IntegerType.get()))),
          optional(5, "data", Types.StringType.get()));

  /** Type of {@code location} in {@link #NESTED_SCHEMA}, for data rows. */
  static final Types.StructType LOCATION_TYPE =
      NESTED_SCHEMA.findField("location").type().asStructType();

  private EqualityDeleteTestUtil() {}

  /** Positional record for {@code schema}; {@code null} values are allowed. */
  static Record record(Schema schema, Object... values) {
    Record record = GenericRecord.create(schema);
    for (int pos = 0; pos < values.length; pos++) {
      record.set(pos, values[pos]);
    }
    return record;
  }

  /**
   * Partition value for writers and file groups; avoids importing the Iceberg Row in Spark tests.
   */
  static StructLike partition(Object... values) {
    return Row.of(values);
  }

  /** Writes one Parquet data file and commits it as an append; returns the committed file. */
  static DataFile appendRows(Table table, StructLike partition, Record... rows) {
    try {
      DataFile dataFile =
          FileHelpers.writeDataFile(
              table, newOutputFile(table), partition, ImmutableList.copyOf(rows));
      table.newAppend().appendFile(dataFile).commit();
      return dataFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Writes one equality delete file keyed by {@code column} and commits it. */
  static DeleteFile addEqualityDeletes(
      Table table, StructLike partition, String column, Object... values) {
    Schema deleteRowSchema = table.schema().select(column);
    List<Record> deletes = Lists.newArrayList();
    for (Object value : values) {
      deletes.add(record(deleteRowSchema, value));
    }

    try {
      DeleteFile deleteFile =
          FileHelpers.writeDeleteFile(
              table, newOutputFile(table), partition, deletes, deleteRowSchema);
      table.newRowDelta().addDeletes(deleteFile).commit();
      return deleteFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Positional record of a struct type, for nested values passed to {@link #record}. */
  static Record struct(Types.StructType type, Object... values) {
    Record record = GenericRecord.create(type);
    for (int pos = 0; pos < values.length; pos++) {
      record.set(pos, values[pos]);
    }
    return record;
  }

  /**
   * Delete row of a delete row schema {@code location: struct<city>} (from {@code
   * table.schema().select("location.city")}) whose location struct is present; pass {@code null}
   * for a present location with a null city. A delete row whose whole location is null is {@code
   * record(deleteRowSchema, (Object) null)}.
   */
  static Record cityDelete(Schema deleteRowSchema, String city) {
    Types.StructType locationType = deleteRowSchema.findField("location").type().asStructType();
    return record(deleteRowSchema, struct(locationType, city));
  }

  /**
   * Writes one equality delete file with explicit equality field IDs and commits it.
   *
   * <p>{@link #addEqualityDeletes(Table, StructLike, String, Object...)} derives the IDs from the
   * top-level columns of the delete row schema, which is the parent struct for a nested key, so
   * nested keys must use this variant.
   */
  static DeleteFile addEqualityDeletes(
      Table table,
      StructLike partition,
      Schema deleteRowSchema,
      int[] equalityFieldIds,
      Record... deletes) {
    GenericAppenderFactory factory =
        new GenericAppenderFactory(
            table, table.schema(), table.spec(), null, equalityFieldIds, deleteRowSchema, null);
    EqualityDeleteWriter<Record> writer =
        factory.newEqDeleteWriter(
            EncryptedFiles.encryptedOutput(newOutputFile(table), EncryptionKeyMetadata.EMPTY),
            FileFormat.PARQUET,
            partition);
    try (Closeable toClose = writer) {
      writer.write(ImmutableList.copyOf(deletes));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    DeleteFile deleteFile = writer.toDeleteFile();
    table.newRowDelta().addDeletes(deleteFile).commit();
    return deleteFile;
  }

  /** Writes one position delete file (format v2) for {@code dataFile} and commits it. */
  static DeleteFile addPositionDeletes(
      Table table, StructLike partition, DataFile dataFile, long... positions) {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    for (long position : positions) {
      deletes.add(Pair.of(dataFile.location(), position));
    }

    try {
      DeleteFile deleteFile =
          FileHelpers.writeDeleteFile(table, newOutputFile(table), partition, deletes, 2).first();
      table.newRowDelta().addDeletes(deleteFile).commit();
      return deleteFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static RewriteFileGroup group(int globalIndex, List<FileScanTask> tasks) {
    FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(globalIndex)
            .partitionIndex(globalIndex)
            .partition(Row.of())
            .build();
    return new RewriteFileGroup(info, Lists.newArrayList(tasks), outputSpecId(tasks), 0L, 0L, 1);
  }

  // mocked tasks return null from spec(); the threshold decision never reads the output spec
  private static int outputSpecId(List<FileScanTask> tasks) {
    PartitionSpec spec = tasks.isEmpty() ? null : tasks.get(0).spec();
    return spec == null ? 0 : spec.specId();
  }

  /**
   * Field ID of a column in a created table: {@code HadoopTables.create} assigns fresh IDs, so
   * tests must resolve the IDs of nested equality keys by name instead of hardcoding them.
   */
  static int fieldId(Table table, String name) {
    Types.NestedField field = table.schema().findField(name);
    Preconditions.checkArgument(
        field != null, "Cannot find field %s in table %s", name, table.name());
    return field.fieldId();
  }

  /** Current planned tasks keyed by data file location. */
  static Map<String, FileScanTask> tasksByLocation(Table table) {
    Map<String, FileScanTask> tasks = Maps.newHashMap();
    for (FileScanTask task : table.newScan().planFiles()) {
      tasks.put(task.file().location(), task);
    }
    return tasks;
  }

  private static OutputFile newOutputFile(Table table) {
    String location =
        table
            .locationProvider()
            .newDataLocation(FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
    return table.io().newOutputFile(location);
  }
}
