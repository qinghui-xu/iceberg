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
package org.apache.iceberg.spark.action;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;

/**
 * Compares reader-local equality-delete filtering with the merge/join path, with one file group per
 * partition (no shared cache) and with many file groups per partition (shared persisted cache).
 *
 * <p>Run with: {@code ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:jmh
 * -PjmhIncludeRegex=IcebergEqualityDeleteCompactionBenchmark
 * -PjmhOutputPath=benchmark/eq-delete-compaction.txt}
 *
 * <p>On JDK 17+, the {@code jmh} task's forked JVM needs the same {@code --add-opens} flags that
 * {@code build.gradle}'s {@code extraJvmArgs} applies to ordinary {@code Test} tasks (for example
 * {@code java.base/sun.nio.ch}), or Spark fails to start with {@code IllegalAccessError}. The
 * {@code jmh} task does not apply {@code extraJvmArgs} to its fork, so pass the same flags through
 * the {@code JDK_JAVA_OPTIONS} environment variable (not {@code JAVA_TOOL_OPTIONS}, which this JDK
 * rejects for {@code --add-opens}) when running this benchmark, e.g. {@code JDK_JAVA_OPTIONS="
 * --add-opens java.base/sun.nio.ch=ALL-UNNAMED ..." ./gradlew ... jmh ...} with every entry from
 * {@code extraJvmArgs}.
 */
@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
public class IcebergEqualityDeleteCompactionBenchmark {

  private static final String[] NAMESPACE = new String[] {"default"};
  private static final String NAME = "eqdeletebench";
  private static final Identifier IDENT = Identifier.of(NAMESPACE, NAME);
  private static final int NUM_FILES = 8;
  private static final long NUM_ROWS = 2_000_000L;
  private static final Schema SCHEMA =
      new Schema(
          required(1, "longCol", Types.LongType.get()),
          optional(2, "intCol", Types.IntegerType.get()),
          optional(3, "stringCol", Types.StringType.get()));

  @Param({"4", "16"})
  private int numDeleteFiles;

  @Param({"100000", "1000000"})
  private int deleteRowsPerFile;

  private final Configuration hadoopConf = new Configuration();
  private SparkSession spark;

  @Setup
  public void setupBench() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", warehouse())
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .master("local[*]");
    spark = builder.getOrCreate();
    Configuration sparkHadoopConf = spark.sessionState().newHadoopConf();
    hadoopConf.forEach(entry -> sparkHadoopConf.set(entry.getKey(), entry.getValue()));
  }

  @TearDown
  public void teardownBench() {
    spark.stop();
  }

  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    initTable();
    appendData();
    appendEqualityDeletes();
  }

  @TearDown(Level.Iteration)
  public void cleanUpIteration() {
    spark.sql("DROP TABLE IF EXISTS " + NAME);
  }

  @Benchmark
  @Threads(1)
  public void readerLocal() {
    rewrite(-1L, Long.MAX_VALUE);
  }

  @Benchmark
  @Threads(1)
  public void mergeJoinSingleGroup() {
    rewrite(0L, Long.MAX_VALUE);
  }

  @Benchmark
  @Threads(1)
  public void mergeJoinManyGroupsSharedCache() {
    // two data files per group -> NUM_FILES / 2 groups sharing one persisted merged delete cache
    rewrite(0L, averageFileSize() * 2 + 1);
  }

  private void rewrite(long joinThresholdRecords, long maxGroupSizeBytes) {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
        .option(
            SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES, Long.toString(maxGroupSizeBytes))
        .option("eq-delete-join-threshold-records", Long.toString(joinThresholdRecords))
        .binPack()
        .execute();
  }

  private void initTable() {
    try {
      SparkSessionCatalog<?> catalog =
          (SparkSessionCatalog<?>)
              Spark3Util.catalogAndIdentifier(spark, "spark_catalog").catalog();
      catalog.dropTable(IDENT);
      catalog.createTable(
          IDENT,
          SparkSchemaUtil.convert(SCHEMA),
          new Transform[0],
          Collections.singletonMap("format-version", "2"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void appendData() {
    Dataset<Row> df =
        spark
            .range(0, NUM_ROWS * NUM_FILES, 1, NUM_FILES)
            .withColumnRenamed("id", "longCol")
            .withColumn(
                "intCol",
                new RandomGeneratingUDF(NUM_ROWS)
                    .randomLongUDF()
                    .apply()
                    .cast(DataTypes.IntegerType))
            .withColumn("stringCol", new RandomGeneratingUDF(NUM_ROWS).randomString().apply());
    df.write().format("iceberg").mode(SaveMode.Append).save(NAME);
  }

  private void appendEqualityDeletes() throws IOException {
    Table table = table();
    Schema deleteSchema = table.schema().select("longCol");
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), new int[] {1}, deleteSchema, null);
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
    Random random = new Random(42);
    RowDelta rowDelta = table.newRowDelta();

    for (int file = 0; file < numDeleteFiles; file++) {
      EqualityDeleteWriter<Record> writer =
          appenderFactory.newEqDeleteWriter(fileFactory.newOutputFile(), FileFormat.PARQUET, null);
      try (EqualityDeleteWriter<Record> closeable = writer) {
        Record record = GenericRecord.create(deleteSchema);
        for (int row = 0; row < deleteRowsPerFile; row++) {
          record.setField("longCol", (long) (random.nextDouble() * NUM_ROWS * NUM_FILES));
          closeable.write(record);
        }
      }

      rowDelta.addDeletes(writer.toDeleteFile());
    }

    rowDelta.commit();
  }

  private long averageFileSize() {
    Table table = table();
    long totalBytes = 0L;
    int files = 0;
    for (FileScanTask task : table.newScan().planFiles()) {
      totalBytes += task.length();
      files += 1;
    }

    return files == 0 ? 0L : totalBytes / files;
  }

  private Table table() {
    try {
      return Spark3Util.loadIcebergTable(spark, NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String warehouse() {
    try {
      return Files.createTempDirectory("benchmark-").toAbsolutePath()
          + "/"
          + UUID.randomUUID()
          + "/";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
