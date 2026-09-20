# Equality-Delete Merge/Join Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each task is executed by a fresh subagent and ends in exactly one commit.

**Goal:** Give `RewriteDataFilesSparkAction` (Spark 3.5 only) an opt-in path that applies equality deletes with distributed, sequence-aware Spark joins against per-cache-key merged delete DataFrames, so hot partitions with many equality deletes no longer OOM executors.

**Architecture:** The action keeps the existing planner and runners. After planning, a driver-side `EqualityDeleteJoinPlan` evaluates the record-count threshold for each `RewriteFileGroup`, computes per-group cache keys (one per equality field-id set, split into partition-scoped and global umbrellas), and builds the group-to-cache dependency maps. `EqualityDeleteCacheManager` builds each merged delete DataFrame at most once, persists it when more than one group needs it, and releases it when its last consumer reaches a terminal outcome or when the action closes. On the join path, `SparkDataFileRewriteRunner` stages the group's scan tasks with equality deletes removed (position deletes and DVs are kept), reads the rows through the existing staged Iceberg scan with the `_file` metadata column, attaches each data file's sequence number and partition scope through a broadcast join, then left-outer joins every merged delete DataFrame with null-safe key equality and keeps rows where `data_sequence_number >= latest_delete_sequence_number` fails to hold. Merged delete DataFrames are produced by reading the equality delete files through the same staged scan, presenting each delete file as a data file. Equality keys are resolved by field ID through `EqualityKeyPath`, which maps a field ID to its column path in the table schema and builds the Spark key expression for both sides of the join, so keys nested in structs are supported and validated before any group is rewritten.

**Tech Stack:** Java 11+ source (build runs on JDK 17/21), Apache Iceberg core/data/spark modules (`spark/v3.5`), Spark 3.5 DataFrame API, JUnit 5 + AssertJ + Mockito, Iceberg `ParameterizedTestExtension`, Gradle.

**Spec:** `compaction_eq_delete_joining_algorithm.md`. Read it before starting any task.

## Global Constraints

- Implement only for Spark 3.5: all production code lives under `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/`, tests under `spark/v3.5/spark/src/test/java/...` and `spark/v3.5/spark-extensions/src/test/java/...`. Do not touch `spark/v3.4`, `spark/v4.0`, `spark/v4.1`, `core/`, `api/`, or `data/`.
- Option name is exactly `eq-delete-join-threshold-records`; default `-1` (join path disabled); negative disables; `0` selects the join path for any group with at least one equality delete file; positive selects it when any single `FileScanTask` accumulates at least that many equality-delete records (spec: "Merge/Join Selection Threshold"). Evaluate with the short-circuit `remaining` algorithm; never compute and retain the exact maximum.
- Second option `eq-delete-join-cache-storage-level`, default `MEMORY_AND_DISK`, parsed with `StorageLevel.fromString`.
- Active equality delete files come only from planned `FileScanTask.deletes()`; never rediscover them from metadata tables.
- File groups are identified by `FileGroupInfo.globalIndex()`, never by `partitionIndex()` or the runner's per-rewrite UUID.
- Each cache key has an independent lifetime; release is per (group, key), exactly once per group, concurrency-safe, idempotent; action-level cleanup unpersists everything left.
- Every merged delete DataFrame uses `max(delete_sequence_number)` per key; a row is deleted only when `data_sequence_number < latest_delete_sequence_number`; key comparisons are null-safe (`eqNullSafe`); keys are resolved by Iceberg field ID. Equality fields may be top-level columns or fields nested in structs; for both, the join path must produce exactly the rows the reader-local path produces, including for rows whose parent struct is null.
- No silent fallback to the reader-local path once the threshold selects the join path.
- Every equality field ID of every equality delete file of a selected group is validated against the current table schema in `EqualityDeleteJoinPlan.plan`, before any file group is rewritten. Fields nested in a list or a map, fields that are lists or maps, and IDs missing from the schema fail the action with a message naming the delete file and the table. This is not a silent fallback either.
- Internal helper columns never reach the written files; the commit path and sequence-number assignment are untouched.
- Follow `CLAUDE.md`: package-private by default, no `Optional`, `Preconditions` first, imports not FQNs, Apache license header on every new file, `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply` before every commit, PR-style commit titles `Spark: ...`.
- New features default to off; all existing `TestRewriteDataFilesAction` tests must also pass with the join path forced on (`eq-delete-join-threshold-records=0`).

## Design Decisions (read before implementing)

1. **Partition scope is carried as a column, not as separate caches.** `BinPackRewriteFilePlanner.groupByPartition` puts every data file whose spec differs from the current spec into one "unpartitioned" group, so after partition evolution one file group can contain files from many `(specId, partition)` scopes. A partition-scoped equality delete written for `(spec 0, c1=1)` must not delete rows from `(spec 0, c1=2)` even when both files sit in the same group. Therefore every partition-scoped merged delete DataFrame carries a `__rewrite_delete_scope_key` column (`specId + "/" + spec.partitionToPath(partition)` of the delete file) in its `groupBy`, and the join adds `data scope == delete scope`. Global deletes (delete files whose spec is unpartitioned, mirroring `DeleteFileIndex`) are merged into separate cache keys without the scope column. This is the spec's "Optional Partition-Domain Mode" applied only where needed; a single-partition group has one constant scope value and pays nothing measurable.
2. **Cache key = (sorted equality field IDs, exact sorted set of delete file locations).** The partition value is implied by the delete files, so it is not part of the key. Two groups anywhere in the table that need exactly the same delete files for the same field IDs share one DataFrame.
3. **Lazy, exactly-once cache build.** A cache entry exists for every planned key from the start; the DataFrame is built on the first `mergedDeletes(key)` call under the entry's lock, persisted and materialized with `count()` only when the key has more than one consumer (spec: create caches only when reuse is expected). Building after the consumer set became empty is a `checkState` failure.
4. **`_data_sequence_number` is attached by a broadcast join on `_file`.** No new core metadata column. The driver knows every data file's location, `dataSequenceNumber()` and scope; a tiny DataFrame `(location, sequence number, scope key)` is broadcast-joined onto the rows.
5. **Equality delete files are read through the existing staged scan.** `EqualityDeleteScans.asDataScanTask` wraps a `DeleteFile` as a `BaseFileScanTask` over a `DataFile` with the same location, format, partition, size, split offsets and key metadata. The standard Iceberg readers project the equality key columns by field ID; the `_file` metadata column identifies the delete file so its sequence number and scope can be attached with the same broadcast join. No new reader classes.
6. **Equality keys are column paths, not column names.** `EqualityKeyPath.of(schema, fieldId)` resolves an equality field ID to its path of field names from the top-level column down to the field (`TypeUtil.indexParents`) and builds the Spark expression used on both sides of the join, so fields nested in structs are supported. Fields nested in a list or a map, fields that are lists or maps, and IDs missing from the current schema are rejected with a clear `IllegalArgumentException`: Iceberg's `StructProjection` cannot project a partial list element or map entry either, and Spark cannot group or join on map values. This is not a silent fallback.
7. **Bin-pack coalesces after the joins.** Joins repartition rows into `spark.sql.shuffle.partitions`; the bin-pack runner coalesces to `group.expectedOutputFiles()` so output file count stays as planned. Sort and z-order runners already repartition.
8. **Scheduling.** Groups run in planner order, reader-local groups first, join groups after (stable order), so groups sharing a partition stay adjacent. Concurrency stays bounded by `max-concurrent-file-group-rewrites`.
9. **Terminal outcome = the runner's `finally`.** The action uses `Tasks...noRetry()`, so any failure of `runner.rewrite(group)` is final; releasing in `finally` satisfies "release on success or final failure". Groups that never start (fail-fast) are released by `EqualityDeleteCacheManager.close()` in the action's try-with-resources.
10. **Nested key expressions mirror `StructProjection`.** The reader-local path compares the *projected struct tree* of each row (`DeleteFilter.applyEqDeletes` wraps rows in `StructProjection.create(requiredSchema, deleteSchema)`): a data row whose parent struct is `NULL` does not match a delete row whose parent struct is present with a `NULL` leaf, while two `NULL` parents do match (`StructProjection.get` returns `null` for a null nested struct). Flattening `a.b.c` to the leaf value would treat `a = NULL` and `a.b.c = NULL` alike and over-delete. Therefore the key expression of a nested path is built bottom-up as `CASE WHEN parent IS NOT NULL THEN struct(child AS name) END` (null otherwise) for every ancestor, giving a struct-typed key whose null-safe equality is exactly the structural equality of the Iceberg projection. Spark supports `<=>`, `groupBy` and equi-join keys on struct types (`ExtractEquiJoinKeys` rewrites `l <=> r` into `coalesce(l, default) = coalesce(r, default)` plus `isnull(l) = isnull(r)`). A top-level field's expression is the column itself. Several equality fields under one parent are ANDed, and the conjunction of per-field struct equalities equals structural equality of the combined projection, so per-field keys are sufficient.
11. **Validate equality fields at planning time.** `GroupJoinInfo.of` calls `EqualityKeyPath.rejectionReason` for every equality field ID of every equality delete file of a selected group, so an unsupported key fails `EqualityDeleteJoinPlan.plan` inside `RewriteDataFilesSparkAction.execute` before any file group is rewritten or committed, with the delete file location and the table name in the message. `EqualityDeleteScans.keyPaths` resolves the same paths again at build time as a second line of defense.

## File Structure

New production files (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/`):

| File | Responsibility |
|------|----------------|
| `DeleteCacheKey.java` | Value object identifying one merged delete DataFrame: sorted field IDs + exact sorted delete file set + `partitionScoped` flag. |
| `EqualityDeleteJoinPlan.java` | Driver-side planning: threshold decision, per-group `GroupJoinInfo` (data files, partition-scoped keys, global keys), validation of every equality field ID against the table schema, dependency maps, group ordering, `scopeKey()`. |
| `EqualityKeyPath.java` | Column path of an equality field in the table schema (`TypeUtil.indexParents`), rejection reasons for unsupported fields, the Spark key expression for either side of the join, and `quoted()`. |
| `EqualityDeleteScans.java` | Spark scan construction: position-delete-only tasks, delete-file-as-data tasks, key paths, file attribute DataFrame, merged delete DataFrame. Owns helper column name constants. |
| `EqualityDeleteCacheManager.java` | Cache entries, lazy exactly-once build, persist/materialize, per-key release, `close()`. |
| `EqualityDeleteJoinFilter.java` | Applies the broadcast attribute join and the null-safe left-outer joins with the sequence predicate; drops helper columns. |

Modified production files:

| File | Change |
|------|--------|
| `SparkDataFileRewriteRunner.java` | Options, `readGroup()` hook, position-delete-only staging, terminal callback. |
| `SparkBinPackFileRewriteRunner.java` | Use `readGroup()`, coalesce on the join path. |
| `SparkShufflingFileRewriteRunner.java` | Use `readGroup()`. |
| `RewriteDataFilesSparkAction.java` | Build the join plan, order groups, own the cache manager lifecycle. |
| `docs/docs/spark-procedures.md` | Document the two options. |

New test files (`spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/`): `EqualityDeleteTestUtil.java`, `TestEqualityDeleteJoinPlan.java`, `TestEqualityKeyPath.java`, `TestEqualityDeleteScans.java`, `TestEqualityDeleteCacheManager.java`, `TestEqualityDeleteJoinFilter.java`. Modified tests: `TestSparkFileRewriteRunners.java`, `TestRewriteDataFilesAction.java`, `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestRewriteDataFilesProcedure.java`.

The untracked placeholder `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/PositionDeleteFilteredDataTable.java` (an empty class) is not used by this design; Task 6 deletes it.

## Commands

```bash
# compile the Spark 3.5 module
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:compileJava :iceberg-spark:iceberg-spark-3.5_2.12:compileTestJava
# format
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply
# one test class
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteJoinPlan"
# the compaction e2e suite (slow, several minutes)
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction"
# procedure tests
./gradlew :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests "org.apache.iceberg.spark.extensions.TestRewriteDataFilesProcedure"
```

Test scaffolding facts every task relies on:

- `org.apache.iceberg.spark.TestBase` starts a local Spark session (`spark`) and a Hive metastore; extend it whenever a test needs `Dataset`/`SparkSession`.
- `org.apache.iceberg.data.FileHelpers` (iceberg-data test artifacts, on the Spark test classpath) writes data files, equality delete files (`writeDeleteFile(table, out, partition, List<Record>, deleteRowSchema)`, equality field IDs = the columns of `deleteRowSchema`, writer spec = `table.spec()`), and position delete files (`writeDeleteFile(table, out, partition, List<Pair<CharSequence, Long>>, formatVersion)` returning `Pair<DeleteFile, CharSequenceSet>`).
- `org.apache.iceberg.TestHelpers.Row.of(...)` builds a `StructLike` partition value; `org.apache.iceberg.spark.data.TestHelpers.deleteFiles(table)` returns the delete files attached by `planFiles()` (with data sequence numbers set).
- `DeleteFile`/`DataFile` objects returned by writers have `dataSequenceNumber() == null`; sequence numbers are only present on files obtained from `table.newScan().planFiles()`.
- `RewriteFileGroup` has a public constructor `(FileGroupInfo, List<FileScanTask>, int outputSpecId, long writeMaxFileSize, long inputSplitSize, int expectedOutputFiles)`; `FileGroupInfo` is built with `ImmutableRewriteDataFiles.FileGroupInfo.builder().globalIndex(i).partitionIndex(i).partition(Row.of()).build()`.
- `FileHelpers.writeDeleteFile(table, out, partition, deletes, deleteRowSchema)` derives the equality field IDs from the *top-level* columns of the delete row schema. For a nested key (`table.schema().select("location.city")` gives `location: struct<city>`) that would record the parent struct's ID, so nested-key tests write delete files through `EqualityDeleteTestUtil.addEqualityDeletes(Table, StructLike, Schema, int[] equalityFieldIds, Record...)`, which uses `GenericFileWriterFactory` with explicit IDs. `Schema.select` accepts nested names and returns the pruned parent struct.
- A Spark `Row` nested in a result row is converted by `rowsToJava` to a nested `Object[]`, and `assertEquals(String, List<Object[]>, List<Object[]>)` compares nested arrays recursively, so `row(1, row("paris", 75000), "a")` is the expected value of a row with a struct column, `row(2, null, "b")` one whose struct is null, and `row(row((Object) null), 2L)` a struct-typed key whose only field is null.

---

### Task 1: Runner options for the merge/join path

**Files:**
- Modify: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkDataFileRewriteRunner.java`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestSparkFileRewriteRunners.java`

**Interfaces:**
- Consumes: `SparkRewriteRunner.validOptions()/init(Map)`, `PropertyUtil.propertyAsLong/propertyAsString`, `org.apache.spark.storage.StorageLevel.fromString`.
- Produces (used by Tasks 6, 7, 8):
  - `public static final String EQ_DELETE_JOIN_THRESHOLD_RECORDS = "eq-delete-join-threshold-records"`
  - `public static final long EQ_DELETE_JOIN_THRESHOLD_RECORDS_DEFAULT = -1L`
  - `public static final String EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL = "eq-delete-join-cache-storage-level"`
  - `public static final String EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL_DEFAULT = "MEMORY_AND_DISK"`
  - `long eqDeleteJoinThresholdRecords()` and `StorageLevel eqDeleteJoinCacheStorageLevel()` (package-private getters)

- [ ] **Step 1: Write the failing tests**

Replace the two `validOptions` assertions in `TestSparkFileRewriteRunners` and add three tests. The full new content of the class body after `removeTable()`:

```java
  @Test
  public void testInvalidConstructorUsagesSortData() {
    // unchanged
  }

  @Test
  public void testInvalidConstructorUsagesZOrderData() {
    // unchanged
  }

  @Test
  public void testBinPackDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackFileRewriteRunner rewriter = new SparkBinPackFileRewriteRunner(spark, table);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .containsExactlyInAnyOrder(
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_THRESHOLD_RECORDS,
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL);
  }

  @Test
  public void testSortDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkSortFileRewriteRunner rewriter = new SparkSortFileRewriteRunner(spark, table, SORT_ORDER);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .containsExactlyInAnyOrder(
            SparkShufflingFileRewriteRunner.SHUFFLE_PARTITIONS_PER_FILE,
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_THRESHOLD_RECORDS,
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL);
  }

  @Test
  public void testZOrderDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderFileRewriteRunner rewriter =
        new SparkZOrderFileRewriteRunner(spark, table, zOrderCols);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .containsExactlyInAnyOrder(
            SparkZOrderFileRewriteRunner.SHUFFLE_PARTITIONS_PER_FILE,
            SparkZOrderFileRewriteRunner.MAX_OUTPUT_SIZE,
            SparkZOrderFileRewriteRunner.VAR_LENGTH_CONTRIBUTION,
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_THRESHOLD_RECORDS,
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL);
  }

  @Test
  public void testEqualityDeleteJoinOptionDefaults() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackFileRewriteRunner rewriter = new SparkBinPackFileRewriteRunner(spark, table);
    rewriter.init(ImmutableMap.of());

    assertThat(rewriter.eqDeleteJoinThresholdRecords())
        .as("Join path must be disabled by default")
        .isEqualTo(SparkDataFileRewriteRunner.EQ_DELETE_JOIN_THRESHOLD_RECORDS_DEFAULT)
        .isNegative();
    assertThat(rewriter.eqDeleteJoinCacheStorageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());
  }

  @Test
  public void testEqualityDeleteJoinOptionParsing() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackFileRewriteRunner rewriter = new SparkBinPackFileRewriteRunner(spark, table);
    rewriter.init(
        ImmutableMap.of(
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_THRESHOLD_RECORDS, "1000",
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL, "DISK_ONLY"));

    assertThat(rewriter.eqDeleteJoinThresholdRecords()).isEqualTo(1000L);
    assertThat(rewriter.eqDeleteJoinCacheStorageLevel()).isEqualTo(StorageLevel.DISK_ONLY());
  }

  @Test
  public void testInvalidEqualityDeleteJoinStorageLevel() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkBinPackFileRewriteRunner rewriter = new SparkBinPackFileRewriteRunner(spark, table);

    Map<String, String> invalidOptions =
        ImmutableMap.of(SparkDataFileRewriteRunner.EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL, "NOT_A_LEVEL");
    assertThatThrownBy(() -> rewriter.init(invalidOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse 'eq-delete-join-cache-storage-level' value NOT_A_LEVEL");
  }

  @Test
  public void testInvalidValuesForZOrderDataOptions() {
    // unchanged
  }
```

Add `import org.apache.spark.storage.StorageLevel;` to the test imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestSparkFileRewriteRunners"`
Expected: compilation FAILS (`EQ_DELETE_JOIN_THRESHOLD_RECORDS` cannot be resolved).

- [ ] **Step 3: Implement the options**

Replace `SparkDataFileRewriteRunner.java` with:

```java
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
   * Maximum number of applicable equality-delete records the reader-local delete filter should
   * handle for one scan task. When any scan task of a file group references equality delete files
   * whose record counts add up to this value or more, the group is rewritten with the merge/join
   * path: equality deletes are merged per equality field-id set into Spark DataFrames and applied
   * with sequence-aware joins instead of being loaded into executor memory.
   *
   * <p>A negative value disables the merge/join path. Zero selects it for every file group that
   * has at least one equality delete file. Position deletes and deletion vectors never count.
   */
  public static final String EQ_DELETE_JOIN_THRESHOLD_RECORDS = "eq-delete-join-threshold-records";

  public static final long EQ_DELETE_JOIN_THRESHOLD_RECORDS_DEFAULT = -1L;

  /**
   * Spark storage level used to persist merged equality-delete DataFrames that are shared by more
   * than one file group when the merge/join path is selected.
   */
  public static final String EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL =
      "eq-delete-join-cache-storage-level";

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
    String name =
        PropertyUtil.propertyAsString(
            options,
            EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL,
            EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL_DEFAULT);
    try {
      return StorageLevel.fromString(name.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Cannot parse '%s' value %s as a Spark storage level",
              EQ_DELETE_JOIN_CACHE_STORAGE_LEVEL,
              name),
          e);
    }
  }
}
```

`SparkShufflingFileRewriteRunner.validOptions()` and `init()` already call `super`, so sort and z-order pick up both options without changes. `SparkBinPackFileRewriteRunner` inherits them.

- [ ] **Step 4: Format and run the tests**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply && ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestSparkFileRewriteRunners"`
Expected: PASS (all 9 tests).

- [ ] **Step 5: Commit**

```bash
git add spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkDataFileRewriteRunner.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestSparkFileRewriteRunners.java
git commit -m "Spark: Add equality-delete merge/join options to data file rewrite runners

Adds eq-delete-join-threshold-records (default -1, disabled) and
eq-delete-join-cache-storage-level (default MEMORY_AND_DISK) to the
Spark 3.5 data file rewrite runners. Behavior is unchanged; later
commits use the options to select the merge/join compaction path."
```

---
### Task 2: Cache keys, equality key paths and the driver-side join plan

**Files:**
- Create: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteCacheKey.java`
- Create: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityKeyPath.java`
- Create: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteJoinPlan.java`
- Create: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/EqualityDeleteTestUtil.java`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityKeyPath.java`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteJoinPlan.java`

**Interfaces:**
- Consumes: `RewriteFileGroup.fileScanTasks()/info().globalIndex()`, `FileScanTask.file()/deletes()/spec()`, `DeleteFile.content()/recordCount()/equalityFieldIds()/specId()/partition()/dataSequenceNumber()/location()`, `Table.specs()/name()/schema()`, `PartitionSpec.partitionToPath`, `org.apache.iceberg.util.DeleteFileSet`, `Schema.findField(int)/findColumnName(int)`, `TypeUtil.indexParents(Types.StructType)` (child field ID to parent field ID; list elements and map keys/values map to the list/map field), Spark `Column.getField/isNotNull`, `functions.when/struct`.
- Produces:
  - `DeleteCacheKey.of(Collection<Integer> equalityFieldIds, Collection<DeleteFile> deleteFiles, boolean partitionScoped)`; getters `equalityFieldIds()` (sorted `List<Integer>`), `deleteFiles()` (sorted by location, distinct), `locations()`, `partitionScoped()`; `equals/hashCode` on `(equalityFieldIds, locations)`.
  - `EqualityDeleteJoinPlan.empty()`, `plan(Table, Iterable<RewriteFileGroup>, long thresholdRecords)`, `static boolean useMergeJoin(RewriteFileGroup, long)`, `isEmpty()`, `usesJoin(int globalIndex)`, `GroupJoinInfo joinInfo(int globalIndex)`, `Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup()`, `Set<Integer> consumers(DeleteCacheKey)`, `Set<DeleteCacheKey> cacheKeys()`, `List<RewriteFileGroup> orderGroups(Iterable<RewriteFileGroup>)`, `static String scopeKey(PartitionSpec, StructLike)`.
  - `EqualityDeleteJoinPlan.GroupJoinInfo`: `List<DataFile> dataFiles()`, `List<DeleteCacheKey> partitionScopedKeys()`, `List<DeleteCacheKey> globalKeys()`, `Set<DeleteCacheKey> cacheKeys()`, `int equalityDeleteFileCount()`. `plan` throws `IllegalArgumentException` with message `Cannot use the equality-delete join path for delete file <location> of table <name>: <rejection reason>` for the first unsupported equality field of a selected group.
  - `EqualityKeyPath` (used by Tasks 3 and 5): `static String rejectionReason(Schema, int fieldId)` (`null` when the field can be a join key; otherwise `"equality field %s is not in the table schema"`, `"equality field %s (%s) is a list"` / `"... is a map"`, `"equality field %s (%s) is nested in list %s"` / `"... nested in map %s"`, the parenthesised `%s` being `Schema.findColumnName`); `static EqualityKeyPath of(Schema, int)` (`IllegalArgumentException` with that reason); `static List<EqualityKeyPath> of(Schema, List<Integer>)` (input order); `int fieldId()`; `List<String> segments()` (top-level column first); `boolean isNested()`; `String toString()` (segments joined with `.`); `Column column(Function<String, Column> rootColumn)` (the key expression; `rootColumn` receives the backtick-quoted top-level column name and is `Dataset::col` to bind the expression to one side of a join, or `functions::col`); `static String quoted(String)`.
  - Test helper `EqualityDeleteTestUtil` (package-private, reused by Tasks 3, 5, 7): `SCHEMA`, `NESTED_SCHEMA` (`id`, `location: struct<city, zip>`, `data`), `LOCATION_TYPE`, `record(Schema, Object...)`, `struct(Types.StructType, Object...)`, `partition(Object...)` (a `StructLike`), `appendRows(Table, StructLike, Record...)`, `addEqualityDeletes(Table, StructLike, String column, Object... values)`, `addEqualityDeletes(Table, StructLike, Schema deleteRowSchema, int[] equalityFieldIds, Record... deletes)`, `cityDelete(Schema deleteRowSchema, String city)`, `addPositionDeletes(Table, StructLike, DataFile, long... positions)`, `group(int globalIndex, List<FileScanTask>)`, `tasksByLocation(Table)`.

- [ ] **Step 1: Create the shared test utility**

```java
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.OutputFile;
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

  /** Partition value for writers and file groups; avoids importing the Iceberg Row in Spark tests. */
  static StructLike partition(Object... values) {
    return Row.of(values);
  }

  /** Writes one Parquet data file and commits it as an append; returns the committed file. */
  static DataFile appendRows(Table table, StructLike partition, Record... rows) {
    try {
      DataFile dataFile =
          FileHelpers.writeDataFile(table, newOutputFile(table), partition, ImmutableList.copyOf(rows));
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
    FileWriterFactory<Record> factory =
        GenericFileWriterFactory.builderFor(table)
            .equalityDeleteRowSchema(deleteRowSchema)
            .equalityFieldIds(equalityFieldIds)
            .build();
    EqualityDeleteWriter<Record> writer =
        factory.newEqualityDeleteWriter(
            EncryptedFiles.encryptedOutput(newOutputFile(table), EncryptionKeyMetadata.EMPTY),
            table.spec(),
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
```

Add `import org.apache.iceberg.PartitionSpec;`, `java.io.Closeable`, `org.apache.iceberg.data.GenericFileWriterFactory`, `org.apache.iceberg.deletes.EqualityDeleteWriter`, `org.apache.iceberg.encryption.EncryptedFiles`, `org.apache.iceberg.encryption.EncryptionKeyMetadata` and `org.apache.iceberg.io.FileWriterFactory` to the utility's imports.

- [ ] **Step 2: Write the failing planner tests**

```java
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.SCHEMA;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addEqualityDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addPositionDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.appendRows;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.group;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.record;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.partition;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
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
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task(eqDelete(0L)))), 0L))
        .isTrue();
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(2, ImmutableList.of(task(posDelete(5L)))), 0L))
        .as("Zero selects the join path only for groups with equality deletes")
        .isFalse();
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(3, ImmutableList.of(task())), 0L)).isFalse();
  }

  @Test
  public void recordCountEqualToThresholdSelectsMergeJoin() {
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task(eqDelete(100L)))), 100L))
        .isTrue();
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(2, ImmutableList.of(task(eqDelete(99L)))), 100L))
        .isFalse();
  }

  @Test
  public void recordCountsAccumulateAcrossDeleteFilesAndFieldIdSets() {
    FileScanTask task = task(eqDelete(40L, 1), eqDelete(30L, 2), eqDelete(30L, 1, 2));
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), 100L)).isTrue();
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), 101L)).isFalse();
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
    assertThat(EqualityDeleteJoinPlan.useMergeJoin(group(1, ImmutableList.of(task)), Long.MAX_VALUE))
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
    assertThat(info.dataFiles()).extracting(DataFile::location)
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
    assertThat(EqualityDeleteJoinPlan.empty().orderGroups(planned)).containsExactly(joinGroup, plainGroup, posDeleteGroup);
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
    assertThat(EqualityDeleteJoinPlan.scopeKey(table.spec(), partition("a"))).isEqualTo("0/category=a");
    Table unpartitioned = unpartitionedTable();
    assertThat(EqualityDeleteJoinPlan.scopeKey(unpartitioned.spec(), partition())).isEqualTo("0/");
  }

  @Test
  public void cacheKeyEqualityIgnoresOrderAndDuplicates() {
    DeleteFile first = eqDelete(1L, 1, 2);
    when(first.location()).thenReturn("s3://bucket/a.parquet");
    DeleteFile second = eqDelete(1L, 1, 2);
    when(second.location()).thenReturn("s3://bucket/b.parquet");

    DeleteCacheKey key1 = DeleteCacheKey.of(ImmutableList.of(2, 1), ImmutableList.of(second, first, first), true);
    DeleteCacheKey key2 = DeleteCacheKey.of(ImmutableList.of(1, 2), ImmutableList.of(first, second), true);

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
    addEqualityDeletes(
        table, null, deleteRowSchema, new int[] {3}, cityDelete(deleteRowSchema, "paris"));
    RewriteFileGroup group = group(1, allTasks(table));

    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    assertThat(plan.usesJoin(1)).isTrue();
    assertThat(plan.joinInfo(1).globalKeys()).hasSize(1);
    assertThat(plan.joinInfo(1).globalKeys().get(0).equalityFieldIds()).containsExactly(3);
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
    return new File(tableDir, java.util.UUID.randomUUID().toString()).toURI().toString();
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
```

Replace `java.util.UUID.randomUUID()` with an imported `UUID` before saving. If `table.updateSpec().removeField("category")` fails because the partition field name differs, use `table.spec().fields().get(0).name()`.

The first validation test uses mocks because a real table cannot carry a delete file whose equality field is unknown: dropping the column afterwards would make `planFiles()` itself fail before the plan runs. The unsupported list/map shapes are covered by `TestEqualityKeyPath` below; the plan test checks the context added to the reason. Add the static imports `EqualityDeleteTestUtil.LOCATION_TYPE`, `NESTED_SCHEMA`, `cityDelete`, `struct` and `import org.apache.iceberg.Schema;`.

Also create `TestEqualityKeyPath`:

```java
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestEqualityKeyPath extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "location",
              Types.StructType.of(
                  optional(3, "city", Types.StringType.get()),
                  optional(
                      4,
                      "geo",
                      Types.StructType.of(optional(5, "country", Types.StringType.get()))))),
          optional(
              6,
              "tags",
              Types.ListType.ofOptional(
                  7, Types.StructType.of(optional(8, "name", Types.StringType.get())))),
          optional(
              9,
              "attrs",
              Types.MapType.ofOptional(10, 11, Types.StringType.get(), Types.StringType.get())),
          optional(12, "we.ird", Types.StringType.get()),
          optional(13, "ids", Types.ListType.ofOptional(14, Types.IntegerType.get())));

  // ---- path resolution (no Spark needed) ----

  @Test
  public void topLevelFieldHasSingleSegment() {
    EqualityKeyPath path = EqualityKeyPath.of(SCHEMA, 1);

    assertThat(path.fieldId()).isEqualTo(1);
    assertThat(path.segments()).containsExactly("id");
    assertThat(path.isNested()).isFalse();
    assertThat(path.toString()).isEqualTo("id");
  }

  @Test
  public void nestedFieldsListSegmentsFromTheTopLevelColumn() {
    assertThat(EqualityKeyPath.of(SCHEMA, 3).segments()).containsExactly("location", "city");

    EqualityKeyPath deep = EqualityKeyPath.of(SCHEMA, 5);
    assertThat(deep.segments()).containsExactly("location", "geo", "country");
    assertThat(deep.isNested()).isTrue();
    assertThat(deep.toString()).isEqualTo("location.geo.country");
  }

  @Test
  public void ofListKeepsTheRequestedOrder() {
    List<EqualityKeyPath> paths = EqualityKeyPath.of(SCHEMA, ImmutableList.of(3, 1));
    assertThat(paths).extracting(EqualityKeyPath::fieldId).containsExactly(3, 1);
  }

  @Test
  public void acceptsPrimitiveAndStructFieldsNestedOnlyInStructs() {
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 1)).isNull();
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 2)).as("struct-typed field").isNull();
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 5)).isNull();
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 12)).isNull();
  }

  @Test
  public void rejectsUnknownFields() {
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 99))
        .isEqualTo("equality field 99 is not in the table schema");
    assertThatThrownBy(() -> EqualityKeyPath.of(SCHEMA, 99))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("equality field 99 is not in the table schema");
  }

  @Test
  public void rejectsFieldsNestedInListsOrMaps() {
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 8))
        .isEqualTo("equality field 8 (tags.element.name) is nested in list tags");
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 11))
        .isEqualTo("equality field 11 (attrs.value) is nested in map attrs");
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 14))
        .isEqualTo("equality field 14 (ids.element) is nested in list ids");
  }

  @Test
  public void rejectsListAndMapFields() {
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 6))
        .isEqualTo("equality field 6 (tags) is a list");
    assertThat(EqualityKeyPath.rejectionReason(SCHEMA, 9))
        .isEqualTo("equality field 9 (attrs) is a map");
  }

  @Test
  public void quotedEscapesBackticks() {
    assertThat(EqualityKeyPath.quoted("plain")).isEqualTo("`plain`");
    assertThat(EqualityKeyPath.quoted("a.b")).isEqualTo("`a.b`");
    assertThat(EqualityKeyPath.quoted("we`ird")).isEqualTo("`we``ird`");
  }

  // ---- Spark expressions ----

  @Test
  public void topLevelColumnExpressionIsTheColumnItself() {
    Dataset<Row> rows = spark.sql("SELECT 1 AS id, 'x' AS `we.ird`");

    List<Object[]> result =
        rowsToJava(
            rows.select(
                    EqualityKeyPath.of(SCHEMA, 1).column(rows::col),
                    EqualityKeyPath.of(SCHEMA, 12).column(rows::col))
                .collectAsList());

    assertEquals("Top-level keys are the raw column values", ImmutableList.of(row(1, "x")), result);
  }

  @Test
  public void nestedColumnExpressionMirrorsStructProjection() {
    Dataset<Row> rows =
        spark.sql(
            "SELECT * FROM VALUES "
                + "(1, named_struct('city', 'paris', 'geo', named_struct('country', 'fr'))), "
                + "(2, named_struct('city', CAST(NULL AS STRING), 'geo', named_struct('country', 'fr'))), "
                + "(3, CAST(NULL AS STRUCT<city: STRING, geo: STRUCT<country: STRING>>)), "
                + "(4, named_struct('city', 'rome', 'geo', CAST(NULL AS STRUCT<country: STRING>))) "
                + "AS t(id, location)");
    EqualityKeyPath city = EqualityKeyPath.of(SCHEMA, 3);
    EqualityKeyPath country = EqualityKeyPath.of(SCHEMA, 5);

    Dataset<Row> keys =
        rows.select(
                col("id"),
                city.column(rows::col).as("city_key"),
                country.column(rows::col).as("country_key"))
            .sort("id");

    assertThat(keys.schema().apply("city_key").dataType())
        .as("The key is the projected parent struct, not the leaf value")
        .isEqualTo(new StructType().add("city", DataTypes.StringType));
    assertThat(keys.schema().apply("country_key").dataType())
        .isEqualTo(
            new StructType().add("geo", new StructType().add("country", DataTypes.StringType)));
    assertEquals(
        "A null ancestor yields a null key; a present ancestor yields a struct, even with a null leaf",
        ImmutableList.of(
            row(1, row("paris"), row(row("fr"))),
            row(2, row((Object) null), row(row("fr"))),
            row(3, null, null),
            row(4, row("rome"), row((Object) null))),
        rowsToJava(keys.collectAsList()));
  }

  @Test
  public void nestedKeysAreGroupableAndNullSafeComparable() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(
                2, "location", Types.StructType.of(optional(3, "city", Types.StringType.get()))));
    Dataset<Row> rows =
        spark.sql(
            "SELECT * FROM VALUES "
                + "(1, named_struct('city', CAST(NULL AS STRING))), "
                + "(2, CAST(NULL AS STRUCT<city: STRING>)), "
                + "(3, named_struct('city', CAST(NULL AS STRING))), "
                + "(4, CAST(NULL AS STRUCT<city: STRING>)) "
                + "AS t(id, location)");
    EqualityKeyPath city = EqualityKeyPath.of(schema, 3);

    Dataset<Row> grouped = rows.groupBy(city.column(rows::col).as("key")).count().sort("key");
    assertEquals(
        "Null parents group together and apart from present parents with a null leaf",
        ImmutableList.of(row(null, 2L), row(row((Object) null), 2L)),
        rowsToJava(grouped.collectAsList()));

    Dataset<Row> left = rows.alias("l");
    Dataset<Row> right = rows.alias("r");
    Dataset<Row> matches =
        left.join(right, city.column(left::col).eqNullSafe(city.column(right::col)), "inner")
            .select(left.col("id").as("l"), right.col("id").as("r"))
            .sort("l", "r");
    assertEquals(
        "Null-safe equality on the key matches null parents only with null parents",
        ImmutableList.of(
            row(1, 1), row(1, 3), row(2, 2), row(2, 4), row(3, 1), row(3, 3), row(4, 2), row(4, 4)),
        rowsToJava(matches.collectAsList()));
  }
}
```

`rowsToJava`, `row(...)` and `assertEquals(String, List<Object[]>, List<Object[]>)` come from `SparkTestHelperBase` through `TestBase`; a nested Spark `Row` becomes a nested `Object[]`, so `row((Object) null)` is a struct whose only field is null and a bare `null` is a null struct.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteJoinPlan" --tests "org.apache.iceberg.spark.actions.TestEqualityKeyPath"`
Expected: compilation FAILS (`DeleteCacheKey`, `EqualityKeyPath`, `EqualityDeleteJoinPlan` missing).

- [ ] **Step 4: Implement `DeleteCacheKey` and `EqualityKeyPath`**

```java
package org.apache.iceberg.spark.actions;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.DeleteFileSet;

/**
 * Identifies one merged equality-delete DataFrame: the equality field IDs it is keyed by and the
 * exact set of equality delete files merged into it.
 *
 * <p>Two keys are equal when they merge the same delete file locations for the same field IDs, so
 * every file group that needs the same umbrella of delete files shares one cached DataFrame. The
 * partition is implied by the delete files: partition-scoped delete files only ever appear in scan
 * tasks of their own partition scope, and global delete files apply everywhere.
 */
class DeleteCacheKey {
  private final List<Integer> equalityFieldIds;
  private final List<DeleteFile> deleteFiles;
  private final List<String> locations;
  private final boolean partitionScoped;

  private DeleteCacheKey(
      List<Integer> equalityFieldIds, List<DeleteFile> deleteFiles, boolean partitionScoped) {
    this.equalityFieldIds = equalityFieldIds;
    this.deleteFiles = deleteFiles;
    this.locations =
        deleteFiles.stream().map(DeleteFile::location).collect(ImmutableList.toImmutableList());
    this.partitionScoped = partitionScoped;
  }

  /**
   * @param equalityFieldIds equality field IDs shared by all delete files
   * @param deleteFiles equality delete files to merge; duplicates by location are removed
   * @param partitionScoped true when the files belong to partitioned specs and rows must also match
   *     on the partition scope, false for global deletes from unpartitioned specs
   */
  static DeleteCacheKey of(
      Collection<Integer> equalityFieldIds,
      Collection<DeleteFile> deleteFiles,
      boolean partitionScoped) {
    Preconditions.checkArgument(
        equalityFieldIds != null && !equalityFieldIds.isEmpty(),
        "Cannot create a delete cache key without equality field IDs");
    Preconditions.checkArgument(
        deleteFiles != null && !deleteFiles.isEmpty(),
        "Cannot create a delete cache key without delete files");

    List<Integer> sortedIds = ImmutableList.sortedCopyOf(equalityFieldIds);
    List<DeleteFile> distinctFiles =
        deleteFiles.stream()
            .collect(Collectors.toCollection(DeleteFileSet::create))
            .stream()
            .sorted(Comparator.comparing(DeleteFile::location))
            .collect(ImmutableList.toImmutableList());
    return new DeleteCacheKey(sortedIds, distinctFiles, partitionScoped);
  }

  List<Integer> equalityFieldIds() {
    return equalityFieldIds;
  }

  List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  List<String> locations() {
    return locations;
  }

  boolean partitionScoped() {
    return partitionScoped;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    DeleteCacheKey that = (DeleteCacheKey) other;
    return equalityFieldIds.equals(that.equalityFieldIds) && locations.equals(that.locations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(equalityFieldIds, locations);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("equalityFieldIds", equalityFieldIds)
        .add("deleteFiles", locations.size())
        .add("partitionScoped", partitionScoped)
        .toString();
  }
}
```

And `EqualityKeyPath`:

```java
package org.apache.iceberg.spark.actions;

import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;

/**
 * Column path of an equality field in the table schema and the Spark expression that projects a
 * row onto that field.
 *
 * <p>Equality deletes identify their key columns by field ID, so a key may be a field nested in
 * structs. The reader-local path ({@code DeleteFilter}) compares the {@code StructProjection} of
 * each row onto the key fields: a row whose parent struct is null is not equal to a row whose parent
 * struct is present with a null leaf, while two null parents are equal. The join path must agree,
 * so {@link #column} builds, for every ancestor from the leaf upwards, {@code CASE WHEN parent IS
 * NOT NULL THEN struct(child) END}, which is null for a null parent. The result is a struct-typed
 * key whose null-safe equality is exactly the structural equality of the Iceberg projection. A
 * top-level field's expression is the column itself.
 *
 * <p>Fields nested in a list or a map, and fields that are lists or maps, are not supported:
 * Iceberg cannot project a partial list element or map entry, and Spark cannot group or join on
 * map values. {@link #rejectionReason} reports these cases so callers can fail with context.
 */
class EqualityKeyPath {
  private final int fieldId;
  private final List<String> segments;

  private EqualityKeyPath(int fieldId, List<String> segments) {
    this.fieldId = fieldId;
    this.segments = segments;
  }

  /**
   * Explains why the field cannot be an equality key of the join path, or returns null when it can.
   */
  static String rejectionReason(Schema schema, int fieldId) {
    Types.NestedField field = schema.findField(fieldId);
    if (field == null) {
      return String.format(Locale.ROOT, "equality field %s is not in the table schema", fieldId);
    }

    if (isListOrMap(field.type())) {
      return String.format(
          Locale.ROOT,
          "equality field %s (%s) is a %s",
          fieldId,
          schema.findColumnName(fieldId),
          typeName(field.type()));
    }

    Map<Integer, Integer> parents = TypeUtil.indexParents(schema.asStruct());
    Integer parentId = parents.get(fieldId);
    while (parentId != null) {
      Types.NestedField parent = schema.findField(parentId);
      if (isListOrMap(parent.type())) {
        return String.format(
            Locale.ROOT,
            "equality field %s (%s) is nested in %s %s",
            fieldId,
            schema.findColumnName(fieldId),
            typeName(parent.type()),
            schema.findColumnName(parentId));
      }

      parentId = parents.get(parentId);
    }

    return null;
  }

  /**
   * @throws IllegalArgumentException with {@link #rejectionReason} when the field is not supported
   */
  static EqualityKeyPath of(Schema schema, int fieldId) {
    String reason = rejectionReason(schema, fieldId);
    Preconditions.checkArgument(reason == null, reason);

    Map<Integer, Integer> parents = TypeUtil.indexParents(schema.asStruct());
    List<String> names = Lists.newArrayList();
    Integer currentId = fieldId;
    while (currentId != null) {
      names.add(schema.findField(currentId).name());
      currentId = parents.get(currentId);
    }

    return new EqualityKeyPath(fieldId, ImmutableList.copyOf(Lists.reverse(names)));
  }

  /** Paths of the given fields, in the given order. */
  static List<EqualityKeyPath> of(Schema schema, List<Integer> fieldIds) {
    ImmutableList.Builder<EqualityKeyPath> paths = ImmutableList.builder();
    for (int fieldId : fieldIds) {
      paths.add(of(schema, fieldId));
    }

    return paths.build();
  }

  int fieldId() {
    return fieldId;
  }

  /** Field names from the top-level column down to the equality field. */
  List<String> segments() {
    return segments;
  }

  boolean isNested() {
    return segments.size() > 1;
  }

  /**
   * Spark expression of this key against a row set.
   *
   * @param rootColumn resolves a backtick-quoted top-level column name in the row set; pass {@code
   *     Dataset::col} to bind the expression to one side of a join, or {@code functions::col}
   */
  Column column(Function<String, Column> rootColumn) {
    return projected(rootColumn.apply(quoted(segments.get(0))), 1);
  }

  // parent is the raw column of segments[0..depth-1]; returns its projection onto segments[depth..]
  private Column projected(Column parent, int depth) {
    if (depth == segments.size()) {
      return parent;
    }

    String name = segments.get(depth);
    Column child = projected(parent.getField(name), depth + 1);
    // a CASE without ELSE is null for a null parent and has the type of its single branch
    return when(parent.isNotNull(), struct(child.as(name)));
  }

  /** Backtick-quotes a column name so it can be passed to {@code Dataset.col}. */
  static String quoted(String columnName) {
    return "`" + columnName.replace("`", "``") + "`";
  }

  @Override
  public String toString() {
    return String.join(".", segments);
  }

  private static boolean isListOrMap(Type type) {
    return type.isListType() || type.isMapType();
  }

  private static String typeName(Type type) {
    return type.typeId().name().toLowerCase(Locale.ROOT);
  }
}
```

`TypeUtil.indexParents` maps a list element ID and map key/value IDs to the list/map field ID, so the ancestor walk reaches the list or map field for anything nested in it. `functions.when(condition, struct(...))` without `otherwise` is null for the rows the condition rejects and has the type of its single branch, so both sides of the join produce the same Spark type without any coercion; the alias inside `struct` fixes the field name, which `DataType.sameType` compares.

- [ ] **Step 5: Implement `EqualityDeleteJoinPlan`**

```java
package org.apache.iceberg.spark.actions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DeleteFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver-side plan for the equality-delete merge/join compaction path.
 *
 * <p>Decides which file groups use the merge/join path, derives the {@link DeleteCacheKey cache
 * keys} each group needs (one partition-scoped key and one global key per equality field-id set)
 * and records the many-to-many group-to-cache dependencies. Every equality field ID of the
 * selected groups is validated against the current table schema here, so an unsupported key fails
 * the action before any file group is rewritten. The dependency maps are immutable once the plan
 * is built; file groups are identified by {@code FileGroupInfo.globalIndex()}.
 */
class EqualityDeleteJoinPlan {
  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteJoinPlan.class);
  private static final EqualityDeleteJoinPlan EMPTY =
      new EqualityDeleteJoinPlan(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

  private final Map<Integer, GroupJoinInfo> joinInfoByGroup;
  private final Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup;
  private final Map<DeleteCacheKey, Set<Integer>> consumersByCache;

  private EqualityDeleteJoinPlan(
      Map<Integer, GroupJoinInfo> joinInfoByGroup,
      Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup,
      Map<DeleteCacheKey, Set<Integer>> consumersByCache) {
    this.joinInfoByGroup = joinInfoByGroup;
    this.requiredCachesByGroup = requiredCachesByGroup;
    this.consumersByCache = consumersByCache;
  }

  /** A plan in which every group uses the reader-local path. */
  static EqualityDeleteJoinPlan empty() {
    return EMPTY;
  }

  /**
   * Evaluates the threshold for every group and builds the cache dependencies of the selected ones.
   *
   * @param table the table being rewritten
   * @param groups all planned file groups
   * @param thresholdRecords value of {@code eq-delete-join-threshold-records}
   */
  static EqualityDeleteJoinPlan plan(
      Table table, Iterable<RewriteFileGroup> groups, long thresholdRecords) {
    if (thresholdRecords < 0) {
      return EMPTY;
    }

    Map<Integer, GroupJoinInfo> joinInfos = Maps.newLinkedHashMap();
    Map<Integer, Set<DeleteCacheKey>> requiredCaches = Maps.newLinkedHashMap();
    Map<DeleteCacheKey, Set<Integer>> consumers = Maps.newLinkedHashMap();

    for (RewriteFileGroup group : groups) {
      if (!useMergeJoin(group, thresholdRecords)) {
        continue;
      }

      int groupIndex = group.info().globalIndex();
      GroupJoinInfo info = GroupJoinInfo.of(table, group);
      joinInfos.put(groupIndex, info);
      requiredCaches.put(groupIndex, info.cacheKeys());
      for (DeleteCacheKey key : info.cacheKeys()) {
        consumers.computeIfAbsent(key, ignored -> Sets.newLinkedHashSet()).add(groupIndex);
      }

      LOG.info(
          "Selected the equality-delete merge/join path for file group {} in {}: {} data files, {} equality delete files, {} cache keys",
          groupIndex,
          table.name(),
          info.dataFiles().size(),
          info.equalityDeleteFileCount(),
          info.cacheKeys().size());
    }

    ImmutableMap.Builder<DeleteCacheKey, Set<Integer>> immutableConsumers = ImmutableMap.builder();
    consumers.forEach((key, groupIndexes) -> immutableConsumers.put(key, ImmutableSet.copyOf(groupIndexes)));

    return new EqualityDeleteJoinPlan(
        ImmutableMap.copyOf(joinInfos),
        ImmutableMap.copyOf(requiredCaches),
        immutableConsumers.build());
  }

  /**
   * Returns true when any scan task in the group reaches the equality-delete record threshold.
   *
   * <p>Inspects tasks one at a time and stops at the first qualifying task. Record counts are
   * compared against the remaining threshold to avoid overflow. Position deletes and deletion
   * vectors never count. Delete files are not deduplicated across tasks because the decision
   * estimates the memory pressure of each task independently.
   */
  static boolean useMergeJoin(RewriteFileGroup group, long thresholdRecords) {
    if (thresholdRecords < 0) {
      return false;
    }

    for (FileScanTask task : group.fileScanTasks()) {
      long remaining = thresholdRecords;
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() != FileContent.EQUALITY_DELETES) {
          continue;
        }

        if (thresholdRecords == 0 || delete.recordCount() >= remaining) {
          return true;
        }

        remaining -= delete.recordCount();
      }
    }

    return false;
  }

  boolean isEmpty() {
    return joinInfoByGroup.isEmpty();
  }

  boolean usesJoin(int groupIndex) {
    return joinInfoByGroup.containsKey(groupIndex);
  }

  GroupJoinInfo joinInfo(int groupIndex) {
    Preconditions.checkArgument(
        usesJoin(groupIndex),
        "File group %s does not use the equality-delete join path",
        groupIndex);
    return joinInfoByGroup.get(groupIndex);
  }

  /** Cache keys required by each merge/join group, keyed by the group's global index. */
  Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup() {
    return requiredCachesByGroup;
  }

  /** Global indexes of the groups that need the given cache key. */
  Set<Integer> consumers(DeleteCacheKey key) {
    return consumersByCache.getOrDefault(key, ImmutableSet.of());
  }

  Set<DeleteCacheKey> cacheKeys() {
    return consumersByCache.keySet();
  }

  /**
   * Orders groups for execution: reader-local groups first, merge/join groups after, each keeping
   * the planner's order so groups that share partition-scoped caches stay adjacent.
   */
  List<RewriteFileGroup> orderGroups(Iterable<RewriteFileGroup> groups) {
    List<RewriteFileGroup> readerLocalGroups = Lists.newArrayList();
    List<RewriteFileGroup> joinGroups = Lists.newArrayList();
    for (RewriteFileGroup group : groups) {
      if (usesJoin(group.info().globalIndex())) {
        joinGroups.add(group);
      } else {
        readerLocalGroups.add(group);
      }
    }

    readerLocalGroups.addAll(joinGroups);
    return readerLocalGroups;
  }

  /**
   * Partition scope of a content file: partition-scoped equality deletes apply only to data files
   * with the same spec ID and partition value, which is exactly what this key encodes.
   */
  static String scopeKey(PartitionSpec spec, StructLike partition) {
    return spec.specId() + "/" + spec.partitionToPath(partition);
  }

  /** Everything the join filter needs for one merge/join file group. */
  static class GroupJoinInfo {
    private final List<DataFile> dataFiles;
    private final List<DeleteCacheKey> partitionScopedKeys;
    private final List<DeleteCacheKey> globalKeys;
    private final int equalityDeleteFileCount;

    private GroupJoinInfo(
        List<DataFile> dataFiles,
        List<DeleteCacheKey> partitionScopedKeys,
        List<DeleteCacheKey> globalKeys,
        int equalityDeleteFileCount) {
      this.dataFiles = dataFiles;
      this.partitionScopedKeys = partitionScopedKeys;
      this.globalKeys = globalKeys;
      this.equalityDeleteFileCount = equalityDeleteFileCount;
    }

    static GroupJoinInfo of(Table table, RewriteFileGroup group) {
      List<DataFile> dataFiles = Lists.newArrayList();
      Map<List<Integer>, Set<DeleteFile>> scopedFiles = Maps.newLinkedHashMap();
      Map<List<Integer>, Set<DeleteFile>> globalFiles = Maps.newLinkedHashMap();
      Set<DeleteFile> allEqualityDeletes = DeleteFileSet.create();
      Set<Integer> validatedFieldIds = Sets.newHashSet();

      for (FileScanTask task : group.fileScanTasks()) {
        DataFile file = task.file();
        Preconditions.checkArgument(
            file.dataSequenceNumber() != null,
            "Cannot use the equality-delete join path: data file %s has no data sequence number",
            file.location());
        dataFiles.add(file);

        for (DeleteFile delete : task.deletes()) {
          if (delete.content() != FileContent.EQUALITY_DELETES) {
            continue;
          }

          Preconditions.checkArgument(
              delete.dataSequenceNumber() != null,
              "Cannot use the equality-delete join path: delete file %s has no data sequence number",
              delete.location());
          // each field ID is resolved once per group; every file keyed by it fails the same way
          for (int fieldId : delete.equalityFieldIds()) {
            if (validatedFieldIds.add(fieldId)) {
              String reason = EqualityKeyPath.rejectionReason(table.schema(), fieldId);
              Preconditions.checkArgument(
                  reason == null,
                  "Cannot use the equality-delete join path for delete file %s of table %s: %s",
                  delete.location(),
                  table.name(),
                  reason);
            }
          }

          allEqualityDeletes.add(delete);

          List<Integer> fieldIds = ImmutableList.sortedCopyOf(delete.equalityFieldIds());
          PartitionSpec deleteSpec = table.specs().get(delete.specId());
          Map<List<Integer>, Set<DeleteFile>> target =
              deleteSpec.isUnpartitioned() ? globalFiles : scopedFiles;
          target.computeIfAbsent(fieldIds, ignored -> DeleteFileSet.create()).add(delete);
        }
      }

      return new GroupJoinInfo(
          ImmutableList.copyOf(dataFiles),
          keys(scopedFiles, true),
          keys(globalFiles, false),
          allEqualityDeletes.size());
    }

    private static List<DeleteCacheKey> keys(
        Map<List<Integer>, Set<DeleteFile>> filesByFieldIds, boolean partitionScoped) {
      ImmutableList.Builder<DeleteCacheKey> keys = ImmutableList.builder();
      filesByFieldIds.forEach(
          (fieldIds, files) -> keys.add(DeleteCacheKey.of(fieldIds, files, partitionScoped)));
      return keys.build();
    }

    List<DataFile> dataFiles() {
      return dataFiles;
    }

    List<DeleteCacheKey> partitionScopedKeys() {
      return partitionScopedKeys;
    }

    List<DeleteCacheKey> globalKeys() {
      return globalKeys;
    }

    Set<DeleteCacheKey> cacheKeys() {
      return ImmutableSet.<DeleteCacheKey>builder()
          .addAll(partitionScopedKeys)
          .addAll(globalKeys)
          .build();
    }

    int equalityDeleteFileCount() {
      return equalityDeleteFileCount;
    }
  }
}
```

- [ ] **Step 6: Format and run the tests**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply && ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteJoinPlan" --tests "org.apache.iceberg.spark.actions.TestEqualityKeyPath"`
Expected: PASS (19 planner tests, 11 key path tests). If `nestedColumnExpressionMirrorsStructProjection` fails on the schema assertion only because Spark reports a different nullability for a struct field, compare with `DataType.sameType`, which ignores nullability: `assertThat(actual.sameType(expected)).isTrue()`. Any other difference (field name, leaf type) is a bug in `projected`.

- [ ] **Step 7: Commit**

```bash
git add spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteCacheKey.java \
        spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityKeyPath.java \
        spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteJoinPlan.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/EqualityDeleteTestUtil.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityKeyPath.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteJoinPlan.java
git commit -m "Spark: Plan equality-delete merge/join groups and cache dependencies

Adds the driver-side planning for the merge/join compaction path: the
short-circuit record-count threshold, per-group cache keys split into
partition-scoped and global equality delete umbrellas per field-id set,
and the immutable group-to-cache dependency maps keyed by global index.
Equality fields are resolved by ID to their column path (EqualityKeyPath),
so keys nested in structs are supported and unsupported keys fail the
plan before any group is rewritten."
```

---
### Task 3: Spark scans for the join path (`EqualityDeleteScans`)

**Files:**
- Create: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteScans.java`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteScans.java`

**Interfaces:**
- Consumes: `DeleteCacheKey` (Task 2), `EqualityKeyPath` (Task 2), `EqualityDeleteJoinPlan.scopeKey` (Task 2), `BaseFileScanTask`, `DataFiles.builder`, `SchemaParser`, `PartitionSpecParser`, `ResidualEvaluator.unpartitioned`, `SparkTableCache`, `ScanTaskSetManager`, `SparkReadOptions.SCAN_TASK_SET_ID`, `MetadataColumns.FILE_PATH`.
- Produces:
  - Column name constants: `FILE_COLUMN` (`"_file"`), `LOCATION_COLUMN` (`"__rewrite_file_location"`), `SEQUENCE_NUMBER_COLUMN` (`"__rewrite_data_sequence_number"`), `SCOPE_COLUMN` (`"__rewrite_scope_key"`), `DELETE_KEY_PREFIX` (`"__rewrite_delete_key_"`), `DELETE_SCOPE_COLUMN` (`"__rewrite_delete_scope_key"`), `DELETE_SEQUENCE_NUMBER_COLUMN` (`"__rewrite_delete_sequence_number"`).
  - `static FileScanTask withoutEqualityDeletes(FileScanTask task)`
  - `static FileScanTask asDataScanTask(Table table, DeleteFile deleteFile)`
  - `EqualityDeleteScans(SparkSession spark, Table table)`
  - `List<EqualityKeyPath> keyPaths(List<Integer> equalityFieldIds)`
  - `Dataset<Row> fileAttributes(Iterable<? extends ContentFile<?>> files)` with columns `LOCATION_COLUMN`, `SEQUENCE_NUMBER_COLUMN`, `SCOPE_COLUMN`
  - `Dataset<Row> mergedDeletes(DeleteCacheKey key, String stagingId)` with columns `DELETE_KEY_PREFIX + i` for each field ID in `key.equalityFieldIds()` order, then `DELETE_SCOPE_COLUMN` when `key.partitionScoped()`, then `DELETE_SEQUENCE_NUMBER_COLUMN`. Key column `i` is struct-typed when field `i` is nested in structs (design decision 10). The caller owns `stagingId` and must remove it from `SparkTableCache` and `ScanTaskSetManager` when the DataFrame is no longer needed.

- [ ] **Step 1: Write the failing tests**

```java
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_KEY_PREFIX;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_SCOPE_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_SEQUENCE_NUMBER_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.LOCATION_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.SCOPE_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.SEQUENCE_NUMBER_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.SCHEMA;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addEqualityDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addPositionDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.appendRows;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.record;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.partition;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.tasksByLocation;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
    DataFile dataFile = appendRows(table, null, record(SCHEMA, 1, "a", "x"), record(SCHEMA, 2, "b", "x"));
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

    List<EqualityKeyPath> paths = scans.keyPaths(ImmutableList.of(3, 1));

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
    int[] cityId = new int[] {3};
    // seq 2: paris, and a present location with a null city
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        cityId,
        cityDelete(deleteRowSchema, "paris"),
        cityDelete(deleteRowSchema, null));
    // seq 3: paris again, and a delete whose whole location struct is null
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        cityId,
        cityDelete(deleteRowSchema, "paris"),
        record(deleteRowSchema, (Object) null));
    DeleteCacheKey key =
        DeleteCacheKey.of(ImmutableList.of(3), TestHelpers.deleteFiles(table), false);

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
    DeleteCacheKey key = DeleteCacheKey.of(ImmutableList.of(1), TestHelpers.deleteFiles(table), false);

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
    DeleteCacheKey key = DeleteCacheKey.of(ImmutableList.of(2), TestHelpers.deleteFiles(table), true);

    String stagingId = UUID.randomUUID().toString();
    try {
      Dataset<Row> merged = new EqualityDeleteScans(spark, table).mergedDeletes(key, stagingId);

      assertThat(merged.columns())
          .containsExactly(
              DELETE_KEY_PREFIX + "0", DELETE_SCOPE_COLUMN, DELETE_SEQUENCE_NUMBER_COLUMN);
      assertEquals(
          "Null keys must be kept and scopes must come from the delete file partition",
          ImmutableList.of(
              row(null, "0/category=a", 3L), row(null, "0/category=b", 4L), row("z", "0/category=b", 4L)),
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
    // equality delete keyed by (data, id): column order in the file differs from field ID order
    Schema deleteRowSchema = table.schema().select("data", "id");
    org.apache.iceberg.data.Record delete = EqualityDeleteTestUtil.record(deleteRowSchema, "a", 1);
    commitEqualityDelete(table, deleteRowSchema, delete); // seq 2
    DeleteCacheKey key =
        DeleteCacheKey.of(ImmutableList.of(2, 1), TestHelpers.deleteFiles(table), false);

    String stagingId = UUID.randomUUID().toString();
    try {
      Dataset<Row> merged = new EqualityDeleteScans(spark, table).mergedDeletes(key, stagingId);

      assertThat(key.equalityFieldIds()).containsExactly(1, 2);
      assertThat(merged.columns())
          .containsExactly(DELETE_KEY_PREFIX + "0", DELETE_KEY_PREFIX + "1", DELETE_SEQUENCE_NUMBER_COLUMN);
      assertEquals(
          "Key columns must follow sorted field ID order",
          ImmutableList.of(row(1, "a", 2L)),
          rowsToJava(merged.collectAsList()));
    } finally {
      SparkTableCache.get().remove(stagingId);
      ScanTaskSetManager.get().removeTasks(table, stagingId);
    }
  }

  private void commitEqualityDelete(
      Table table, Schema deleteRowSchema, org.apache.iceberg.data.Record delete) {
    try {
      String location =
          table
              .locationProvider()
              .newDataLocation(
                  org.apache.iceberg.FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
      DeleteFile deleteFile =
          org.apache.iceberg.data.FileHelpers.writeDeleteFile(
              table, table.io().newOutputFile(location), null, ImmutableList.of(delete), deleteRowSchema);
      table.newRowDelta().addDeletes(deleteFile).commit();
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
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
```

Convert the inline fully-qualified names (`org.apache.iceberg.data.Record`, `org.apache.iceberg.data.FileHelpers`, `org.apache.iceberg.FileFormat`, `java.io.IOException`, `java.io.UncheckedIOException`) to imports before saving. Add the static imports `EqualityDeleteTestUtil.LOCATION_TYPE`, `NESTED_SCHEMA`, `cityDelete`, `struct` and the imports `org.apache.spark.sql.types.DataTypes`, `org.apache.spark.sql.types.StructType`; drop the unused `optional`, `required` and `Types` imports. `rowsToJava`, `row(...)` and `assertEquals(String, List<Object[]>, List<Object[]>)` come from `SparkTestHelperBase` through `TestBase`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteScans"`
Expected: compilation FAILS (`EqualityDeleteScans` missing).

- [ ] **Step 3: Implement `EqualityDeleteScans`**

```java
package org.apache.iceberg.spark.actions;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
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
import org.apache.iceberg.MetadataColumns;
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
 * expose the {@code _file} metadata column, which a broadcast join uses to attach the data
 * sequence number and the partition scope of every file.
 */
class EqualityDeleteScans {
  static final String FILE_COLUMN = MetadataColumns.FILE_PATH.name();
  static final String LOCATION_COLUMN = "__rewrite_file_location";
  static final String SEQUENCE_NUMBER_COLUMN = "__rewrite_data_sequence_number";
  static final String SCOPE_COLUMN = "__rewrite_scope_key";
  static final String DELETE_KEY_PREFIX = "__rewrite_delete_key_";
  static final String DELETE_SCOPE_COLUMN = "__rewrite_delete_scope_key";
  static final String DELETE_SEQUENCE_NUMBER_COLUMN = "__rewrite_delete_sequence_number";

  private static final StructType FILE_ATTRIBUTES_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField(LOCATION_COLUMN, DataTypes.StringType, false, Metadata.empty()),
            new StructField(SEQUENCE_NUMBER_COLUMN, DataTypes.LongType, false, Metadata.empty()),
            new StructField(SCOPE_COLUMN, DataTypes.StringType, false, Metadata.empty())
          });

  private final SparkSession spark;
  private final Table table;

  EqualityDeleteScans(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
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
        spec != null, "Cannot find partition spec %s for delete file %s", deleteFile.specId(), deleteFile.location());
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

  /** One row per file with its location, data sequence number and partition scope. */
  Dataset<Row> fileAttributes(Iterable<? extends ContentFile<?>> files) {
    List<Row> rows = Lists.newArrayList();
    for (ContentFile<?> file : files) {
      PartitionSpec spec = table.specs().get(file.specId());
      rows.add(
          RowFactory.create(
              file.location(),
              file.dataSequenceNumber(),
              EqualityDeleteJoinPlan.scopeKey(spec, file.partition())));
    }

    return spark.createDataFrame(rows, FILE_ATTRIBUTES_SCHEMA);
  }

  /**
   * Reads every delete file of the key once and merges the rows by equality key (and partition
   * scope for partition-scoped keys), keeping the latest delete sequence number per key.
   *
   * <p>The delete file scan tasks are staged under {@code stagingId}; the caller must remove that ID
   * from {@link SparkTableCache} and {@link ScanTaskSetManager} once the DataFrame is released.
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
    projection.add(col(FILE_COLUMN));

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
```

If `groupBy` with an aliased column does not name the output column `DELETE_SCOPE_COLUMN` on Spark 3.5, replace the alias with `tagged = tagged.withColumnRenamed(SCOPE_COLUMN, DELETE_SCOPE_COLUMN)` before grouping and group by `col(DELETE_SCOPE_COLUMN)`.

- [ ] **Step 4: Format and run the tests**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply && ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteScans"`
Expected: PASS (8 tests). If `mergedDeletesOfNestedKeysAreStructsMirroringTheProjection` returns the leaf value instead of a struct, the delete-side projection is bypassing `EqualityKeyPath.column`.

- [ ] **Step 5: Commit**

```bash
git add spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteScans.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteScans.java
git commit -m "Spark: Read equality delete files as merged DataFrames for compaction

Adds EqualityDeleteScans, which strips equality deletes from planned scan
tasks, reads equality delete files through the staged Iceberg scan by
presenting them as data files, and merges them per equality key (and
partition scope) with the latest delete sequence number. Key columns are
EqualityKeyPath expressions resolved by field ID, so keys nested in
structs are compared as the reader-local path compares them."
```

---
### Task 4: Cache lifecycle (`EqualityDeleteCacheManager`)

**Files:**
- Create: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteCacheManager.java`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteCacheManager.java`

**Interfaces:**
- Consumes: `DeleteCacheKey` (Task 2), `EqualityDeleteJoinPlan.requiredCachesByGroup()` (Task 2), `EqualityDeleteScans.mergedDeletes(key, stagingId)` (Task 3), `SparkTableCache`, `ScanTaskSetManager`, `StorageLevel`.
- Produces:
  - `EqualityDeleteCacheManager(SparkSession spark, Table table, EqualityDeleteJoinPlan plan, StorageLevel storageLevel)` (production)
  - `@VisibleForTesting EqualityDeleteCacheManager(Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup, StorageLevel storageLevel, BiFunction<DeleteCacheKey, String, Dataset<Row>> builder, Consumer<String> unstage)`
  - `Dataset<Row> mergedDeletes(DeleteCacheKey key)` — builds once per key on first call; persists + `count()` only when the key has more than one planned consumer; `IllegalStateException` if the key has no remaining consumers.
  - `void onGroupTerminal(int groupIndex)` — idempotent; releases one reference per key of the group; unpersists and unstages on the last consumer.
  - `void close()` — releases everything still present.
  - Test accessors: `int size()`, `boolean isBuilt(DeleteCacheKey)`, `boolean isPersisted(DeleteCacheKey)`, `Set<Integer> remainingConsumers(DeleteCacheKey)`.

- [ ] **Step 1: Write the failing tests**

```java
package org.apache.iceberg.spark.actions;

import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<DeleteCacheKey, AtomicInteger> builds = new ConcurrentHashMap<>();
    private final Map<DeleteCacheKey, Dataset<Row>> dataFrames = new ConcurrentHashMap<>();
    private final Map<DeleteCacheKey, String> stagingIds = new ConcurrentHashMap<>();
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
        manager(builder, ImmutableMap.of(1, ImmutableSet.of(KEY_A), 2, ImmutableSet.of(KEY_A, KEY_B)));
    Dataset<Row> cacheA = manager.mergedDeletes(KEY_A);
    manager.onGroupTerminal(1);

    manager.close();

    assertThat(manager.size()).isZero();
    assertThat(cacheA.storageLevel()).isEqualTo(StorageLevel.NONE());
    assertThat(builder.unstageCount(KEY_A)).isEqualTo(1);
    assertThat(builder.unstaged(KEY_B)).as("Never built, nothing staged").isFalse();
    assertThatThrownBy(() -> manager.mergedDeletes(KEY_A)).isInstanceOf(IllegalStateException.class);
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteCacheManager"`
Expected: compilation FAILS (`EqualityDeleteCacheManager` missing).

- [ ] **Step 3: Implement `EqualityDeleteCacheManager`**

```java
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
 * reaches a terminal outcome, and the DataFrame is unpersisted when the last consumer releases. {@link
 * #close()} releases anything left after abnormal termination.
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
        entry != null,
        "Cannot load merged equality deletes for %s: no remaining consumers",
        key);
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
```

- [ ] **Step 4: Format and run the tests**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply && ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteCacheManager"`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteCacheManager.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteCacheManager.java
git commit -m "Spark: Manage merged equality-delete DataFrame lifecycle for compaction

Adds EqualityDeleteCacheManager: one lazily built entry per cache key,
persisted and materialized only when shared, released per key when the
last consuming file group reaches a terminal outcome, and cleaned up on
close. Release is idempotent per group and safe across rewrite threads."
```

---

### Task 5: Applying the joins (`EqualityDeleteJoinFilter`)

**Files:**
- Create: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteJoinFilter.java`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteJoinFilter.java`

**Interfaces:**
- Consumes: `EqualityDeleteScans` (Task 3: constants, `fileAttributes`, `keyPaths`), `EqualityKeyPath.column` (Task 2), `EqualityDeleteCacheManager.mergedDeletes` (Task 4), `EqualityDeleteJoinPlan.GroupJoinInfo` (Task 2).
- Produces: `EqualityDeleteJoinFilter(EqualityDeleteScans scans, EqualityDeleteCacheManager caches)` and `Dataset<Row> filter(Dataset<Row> dataRows, GroupJoinInfo info)`. Input must contain the table columns plus `_file`; output contains exactly the input columns without `_file`, in the same order.

- [ ] **Step 1: Write the failing tests**

```java
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.SCHEMA;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addEqualityDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.addPositionDeletes;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.appendRows;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.group;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.record;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.partition;
import static org.apache.iceberg.spark.actions.EqualityDeleteTestUtil.tasksByLocation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
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
    appendRows(table, null, record(SCHEMA, 1, "a", "x"), record(SCHEMA, 2, "b", "x"), record(SCHEMA, 3, "c", "x")); // seq 1
    addEqualityDeletes(table, null, "id", 2, 3); // seq 2
    appendRows(table, null, record(SCHEMA, 3, "c2", "y")); // seq 3: newer than the delete, survives

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    EqualityDeleteScans scans = new EqualityDeleteScans(spark, table);

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
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table), caches)
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
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table), caches)
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
    DataFile fileX = appendRows(table, partition("x"), record(SCHEMA, 1, "a", "x"), record(SCHEMA, 5, "e", "x")); // seq 1
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
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertEquals(
          "Global deletes remove older rows in every partition; position deletes are still applied",
          ImmutableList.of(row(3, "a", "z")),
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
    // seq 2: deletes location.city = paris and present locations with a null city
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        new int[] {3},
        cityDelete(deleteRowSchema, "paris"),
        cityDelete(deleteRowSchema, null));
    appendRows(
        table, null, record(NESTED_SCHEMA, 5, struct(LOCATION_TYPE, "paris", 75001), "e")); // seq 3

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table), caches)
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
    // seq 2: a delete row whose whole location struct is null
    addEqualityDeletes(
        table, null, deleteRowSchema, new int[] {3}, record(deleteRowSchema, (Object) null));

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table), caches)
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
    // seq 2: keyed by (id, location.city); only (1, paris) matches
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        new int[] {1, 3},
        record(deleteRowSchema, 1, struct(deleteLocationType, "paris")));

    RewriteFileGroup group = group(1, allTasks(table));
    EqualityDeleteJoinPlan plan = EqualityDeleteJoinPlan.plan(table, ImmutableList.of(group), 0L);
    assertThat(plan.joinInfo(1).globalKeys().get(0).equalityFieldIds()).containsExactly(1, 3);

    try (EqualityDeleteCacheManager caches =
        new EqualityDeleteCacheManager(spark, table, plan, StorageLevel.MEMORY_AND_DISK())) {
      Dataset<Row> survivors =
          new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark, table), caches)
              .filter(readWithoutEqualityDeletes(table, group), plan.joinInfo(1));

      assertEquals(
          "Only the row matching both the top-level and the nested key is removed",
          ImmutableList.of(row(1, row("rome", 100), "b"), row(2, row("paris", 75000), "c")),
          rowsToJava(survivors.sort("id", "data").collectAsList()));
    }
  }

  private Dataset<Row> readWithoutEqualityDeletes(
      Table table, RewriteFileGroup group) {
    this.stagedTable = table;
    this.stagingId = UUID.randomUUID().toString();
    List<FileScanTask> tasks =
        group.fileScanTasks().stream()
            .map(EqualityDeleteScans::withoutEqualityDeletes)
            .collect(Collectors.toList());
    SparkTableCache.get().add(stagingId, table);
    ScanTaskSetManager.get().stageTasks(table, stagingId, tasks);
    return spark
        .read()
        .format("iceberg")
        .option(SparkReadOptions.SCAN_TASK_SET_ID, stagingId)
        .load(stagingId)
        .selectExpr("*", EqualityDeleteScans.FILE_COLUMN);
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
```

Partition values come from `EqualityDeleteTestUtil.partition(...)` so that only `org.apache.spark.sql.Row` needs importing here. Add the static imports `EqualityDeleteTestUtil.LOCATION_TYPE`, `NESTED_SCHEMA`, `cityDelete`, `struct` and the imports `org.apache.iceberg.Schema`, `org.apache.iceberg.types.Types`. The reader-local comparisons read the table through the regular Spark scan, which applies the same delete files with `DeleteFilter`; they are the executable form of the "observably equivalent" constraint. If the reader-local read itself fails for nested keys, that is a reader bug outside this plan: report it with the stack trace and do not weaken the assertion.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteJoinFilter"`
Expected: compilation FAILS (`EqualityDeleteJoinFilter` missing).

- [ ] **Step 3: Implement `EqualityDeleteJoinFilter`**

```java
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_KEY_PREFIX;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_SCOPE_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.DELETE_SEQUENCE_NUMBER_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.FILE_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.LOCATION_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.SCOPE_COLUMN;
import static org.apache.iceberg.spark.actions.EqualityDeleteScans.SEQUENCE_NUMBER_COLUMN;
import static org.apache.spark.sql.functions.broadcast;

import java.util.List;
import org.apache.iceberg.spark.actions.EqualityDeleteJoinPlan.GroupJoinInfo;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Applies a file group's equality deletes to data rows with sequence-aware joins.
 *
 * <p>The input rows carry the {@code _file} metadata column. A broadcast join attaches each data
 * file's sequence number and partition scope, then every merged delete DataFrame is left-outer
 * joined with null-safe key equality (plus scope equality for partition-scoped deletes). A row
 * survives a join unless {@code data_sequence_number < latest_delete_sequence_number}. Rows must
 * survive every join to be written. Key expressions come from {@link EqualityKeyPath}, so a key
 * nested in structs is compared as the reader-local {@code StructProjection} compares it: a null
 * parent struct matches only a null parent struct.
 */
class EqualityDeleteJoinFilter {
  private final EqualityDeleteScans scans;
  private final EqualityDeleteCacheManager caches;

  EqualityDeleteJoinFilter(EqualityDeleteScans scans, EqualityDeleteCacheManager caches) {
    this.scans = scans;
    this.caches = caches;
  }

  /**
   * @param dataRows table columns plus {@code _file}, read with equality deletes removed
   * @param info join information of the file group
   * @return the surviving rows with exactly the table columns
   */
  Dataset<Row> filter(Dataset<Row> dataRows, GroupJoinInfo info) {
    Dataset<Row> attributes = scans.fileAttributes(info.dataFiles());
    Dataset<Row> rows =
        dataRows
            .join(
                broadcast(attributes),
                dataRows.col(FILE_COLUMN).equalTo(attributes.col(LOCATION_COLUMN)),
                "inner")
            .drop(LOCATION_COLUMN);

    for (DeleteCacheKey key : info.partitionScopedKeys()) {
      rows = applyDeletes(rows, key);
    }

    for (DeleteCacheKey key : info.globalKeys()) {
      rows = applyDeletes(rows, key);
    }

    return rows.drop(FILE_COLUMN, SEQUENCE_NUMBER_COLUMN, SCOPE_COLUMN);
  }

  private Dataset<Row> applyDeletes(Dataset<Row> rows, DeleteCacheKey key) {
    Dataset<Row> deletes = caches.mergedDeletes(key);
    List<EqualityKeyPath> keyPaths = scans.keyPaths(key.equalityFieldIds());

    Column condition = null;
    for (int pos = 0; pos < keyPaths.size(); pos++) {
      Column match =
          keyPaths.get(pos).column(rows::col).eqNullSafe(deletes.col(DELETE_KEY_PREFIX + pos));
      condition = condition == null ? match : condition.and(match);
    }

    if (key.partitionScoped()) {
      condition = condition.and(rows.col(SCOPE_COLUMN).equalTo(deletes.col(DELETE_SCOPE_COLUMN)));
    }

    Column latestDeleteSequenceNumber = deletes.col(DELETE_SEQUENCE_NUMBER_COLUMN);
    Column survives =
        latestDeleteSequenceNumber
            .isNull()
            .or(rows.col(SEQUENCE_NUMBER_COLUMN).geq(latestDeleteSequenceNumber));

    return rows.join(deletes, condition, "left_outer").filter(survives).drop(deletes.columns());
  }
}
```

- [ ] **Step 4: Format and run the tests**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply && ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDeleteJoinFilter"`
Expected: PASS (7 tests). If the reader-local comparison in the nested tests disagrees on the null-location row, the `CASE WHEN parent IS NOT NULL` guard is missing on one side of the join; fix production code, not the tests. Also re-run Tasks 2 to 4 test classes to make sure nothing regressed:
`./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestEqualityDelete*"`

- [ ] **Step 5: Commit**

```bash
git add spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/EqualityDeleteJoinFilter.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestEqualityDeleteJoinFilter.java
git commit -m "Spark: Apply equality deletes with sequence-aware joins during compaction

Adds EqualityDeleteJoinFilter, which attaches each data file's sequence
number and partition scope through a broadcast join and removes rows
older than the latest matching delete for every merged equality-delete
DataFrame of the file group, using null-safe key comparisons. Keys nested
in structs are compared through their EqualityKeyPath expressions."
```

---
### Task 6: Wire the join path into the runners and the action

**Files:**
- Modify: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkDataFileRewriteRunner.java`
- Modify: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java`
- Modify: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkShufflingFileRewriteRunner.java`
- Modify: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`
- Delete: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/PositionDeleteFilteredDataTable.java` (untracked empty placeholder, unused)
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestRewriteDataFilesAction.java` (parameterize so every existing test also runs with the join path forced on)

**Interfaces:**
- Consumes: everything from Tasks 1 to 5.
- Produces (used by Task 7 tests):
  - `SparkDataFileRewriteRunner.enableEqualityDeleteJoin(EqualityDeleteJoinPlan plan, EqualityDeleteCacheManager caches)`
  - `SparkDataFileRewriteRunner.usesEqualityDeleteJoin(RewriteFileGroup group)`
  - `SparkDataFileRewriteRunner.readGroup(String groupId, RewriteFileGroup group, Map<String, String> readOptions)`
  - `RewriteDataFilesSparkAction` field `runner` typed as `SparkDataFileRewriteRunner`; `doExecute`/`doExecuteWithPartialProgress` take `(plan, List<RewriteFileGroup> groups, commitManager)`.

- [ ] **Step 1: Parameterize the e2e suite (the failing test)**

In `TestRewriteDataFilesAction`, replace the parameter declarations:

```java
  @Parameter(index = 0)
  private int formatVersion;

  @Parameter(index = 1)
  private long eqDeleteJoinThreshold;

  @Parameters(name = "formatVersion = {0}, eqDeleteJoinThreshold = {1}")
  protected static Object[][] parameters() {
    List<Object[]> params = Lists.newArrayList();
    for (int version : org.apache.iceberg.TestHelpers.V2_AND_ABOVE) {
      params.add(
          new Object[] {version, SparkDataFileRewriteRunner.EQ_DELETE_JOIN_THRESHOLD_RECORDS_DEFAULT});
      params.add(new Object[] {version, 0L});
    }

    return params.toArray(new Object[0][]);
  }
```

and make `basicRewrite` pass the threshold:

```java
  private RewriteDataFilesSparkAction basicRewrite(Table table) {
    // Always compact regardless of input files
    table.refresh();
    return actions()
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
        .option(
            SparkDataFileRewriteRunner.EQ_DELETE_JOIN_THRESHOLD_RECORDS,
            String.valueOf(eqDeleteJoinThreshold));
  }
```

Add this helper next to `shouldHaveACleanCache`:

```java
  protected void shouldHaveNoCachedDataFrames() {
    assertThat(spark.sharedState().cacheManager().isEmpty())
        .as("Should not leave persisted merged equality-delete DataFrames behind")
        .isTrue();
  }
```

and call `shouldHaveNoCachedDataFrames();` at the end of `testRemoveDangledEqualityDeletesPartitionEvolution` (the only existing test with equality deletes).

- [ ] **Step 2: Run the suite to see the new parameter set exercise the join path and fail**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction.testRemoveDangledEqualityDeletesPartitionEvolution"`
Expected: the `eqDeleteJoinThreshold = 0` variants PASS trivially for now (the option is accepted but unused). This step confirms the parameterization compiles and both parameter values run. The join path itself is verified after Step 6.

- [ ] **Step 3: Extend `SparkDataFileRewriteRunner`**

Add these imports: `java.util.List`, `java.util.stream.Collectors`, `org.apache.iceberg.relocated.com.google.common.base.Preconditions`, `org.apache.iceberg.spark.SparkReadOptions`, `org.apache.spark.sql.Dataset`, `org.apache.spark.sql.Row`, `org.slf4j.Logger`, `org.slf4j.LoggerFactory`. Add fields and methods:

```java
  private static final Logger LOG = LoggerFactory.getLogger(SparkDataFileRewriteRunner.class);

  private EqualityDeleteJoinPlan joinPlan = EqualityDeleteJoinPlan.empty();
  private EqualityDeleteCacheManager cacheManager = null;
  private EqualityDeleteJoinFilter joinFilter = null;

  /**
   * Enables the merge/join path for the groups selected in the plan. Called by the action once per
   * execution before any group is rewritten; the action owns the cache manager lifecycle.
   */
  void enableEqualityDeleteJoin(EqualityDeleteJoinPlan plan, EqualityDeleteCacheManager caches) {
    Preconditions.checkArgument(plan != null, "Invalid equality-delete join plan: null");
    Preconditions.checkArgument(caches != null, "Invalid equality-delete cache manager: null");
    this.joinPlan = plan;
    this.cacheManager = caches;
    this.joinFilter = new EqualityDeleteJoinFilter(new EqualityDeleteScans(spark(), table()), caches);
  }

  boolean usesEqualityDeleteJoin(RewriteFileGroup group) {
    return joinPlan.usesJoin(group.info().globalIndex());
  }

  @Override
  public Set<DataFile> rewrite(RewriteFileGroup group) {
    String groupId = UUID.randomUUID().toString();
    try {
      tableCache.add(groupId, table());
      taskSetManager.stageTasks(table(), groupId, scanTasks(group));

      doRewrite(groupId, group);

      return coordinator.fetchNewFiles(table(), groupId);
    } finally {
      tableCache.remove(groupId);
      taskSetManager.removeTasks(table(), groupId);
      coordinator.clearRewrite(table(), groupId);
      // the action never retries a group, so leaving this method is the group's terminal outcome
      if (cacheManager != null) {
        cacheManager.onGroupTerminal(group.info().globalIndex());
      }
    }
  }

  /**
   * Reads the staged file group. On the merge/join path the rows are read with equality deletes
   * removed from the scan tasks and filtered against the merged equality-delete DataFrames; the
   * result has exactly the table columns.
   */
  Dataset<Row> readGroup(String groupId, RewriteFileGroup group, Map<String, String> readOptions) {
    Dataset<Row> rows =
        spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .options(readOptions)
            .load(groupId);

    if (!usesEqualityDeleteJoin(group)) {
      return rows;
    }

    EqualityDeleteJoinPlan.GroupJoinInfo info = joinPlan.joinInfo(group.info().globalIndex());
    LOG.info(
        "Rewriting file group {} of {} with the equality-delete merge/join path ({} equality delete files, {} cache keys)",
        group.info().globalIndex(),
        table().name(),
        info.equalityDeleteFileCount(),
        info.cacheKeys().size());
    Dataset<Row> rowsWithFile = rows.selectExpr("*", EqualityDeleteScans.FILE_COLUMN);
    return joinFilter.filter(rowsWithFile, info);
  }

  private List<FileScanTask> scanTasks(RewriteFileGroup group) {
    if (usesEqualityDeleteJoin(group)) {
      return group.fileScanTasks().stream()
          .map(EqualityDeleteScans::withoutEqualityDeletes)
          .collect(Collectors.toList());
    }

    return group.fileScanTasks();
  }
```

- [ ] **Step 4: Route the runners through `readGroup`**

`SparkBinPackFileRewriteRunner.doRewrite` becomes:

```java
  @Override
  protected void doRewrite(String groupId, RewriteFileGroup group) {
    // read the files packing them into splits of the required size
    Map<String, String> readOptions =
        ImmutableMap.of(
            SparkReadOptions.SPLIT_SIZE, String.valueOf(group.inputSplitSize()),
            SparkReadOptions.FILE_OPEN_COST, "0");
    Dataset<Row> scanDF = readGroup(groupId, group, readOptions);

    if (usesEqualityDeleteJoin(group)) {
      // the joins repartition rows into spark.sql.shuffle.partitions; coalesce so each output
      // partition becomes one file of the planned size, as splits do on the reader-local path
      scanDF = scanDF.coalesce(Math.max(1, group.expectedOutputFiles()));
    }

    // write the packed data into new files where each split becomes a new file
    scanDF
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, group.maxOutputFileSize())
        .option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
        .option(SparkWriteOptions.OUTPUT_SPEC_ID, group.outputSpecId())
        .mode("append")
        .save(groupId);
  }
```

(add imports `java.util.Map` and `org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap`).

`SparkShufflingFileRewriteRunner.doRewrite`: replace the `spark().read()...load(groupId)` block with

```java
    Dataset<Row> scanDF = readGroup(groupId, fileGroup, ImmutableMap.of());
```

(import `org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap`; `SparkReadOptions` import can be dropped if unused).

- [ ] **Step 5: Own the plan and cache lifecycle in the action**

In `RewriteDataFilesSparkAction`:

1. Change the field type: `private SparkDataFileRewriteRunner runner = null;` (the three `binPack/sort/zOrder` assignments and `init` already create Spark runners). Remove the now-unused `FileRewriteRunner` import if the compiler flags it.
2. Add import `org.apache.iceberg.relocated.com.google.common.collect.Lists`.
3. Replace the body of `execute()` from `Builder resultBuilder = ...` to `ImmutableRewriteDataFiles.Result result = resultBuilder.build();` with:

```java
    List<RewriteFileGroup> plannedGroups = Lists.newArrayList(plan.groups());
    EqualityDeleteJoinPlan joinPlan =
        EqualityDeleteJoinPlan.plan(table, plannedGroups, runner.eqDeleteJoinThresholdRecords());
    List<RewriteFileGroup> groups = joinPlan.orderGroups(plannedGroups);

    Builder resultBuilder;
    try (EqualityDeleteCacheManager cacheManager =
        new EqualityDeleteCacheManager(
            spark(), table, joinPlan, runner.eqDeleteJoinCacheStorageLevel())) {
      runner.enableEqualityDeleteJoin(joinPlan, cacheManager);
      resultBuilder =
          partialProgressEnabled
              ? doExecuteWithPartialProgress(plan, groups, commitManager(startingSnapshotId))
              : doExecute(plan, groups, commitManager(startingSnapshotId));
    }

    ImmutableRewriteDataFiles.Result result = resultBuilder.build();
```

4. Change both executors to accept the ordered list and iterate it instead of `plan.groups()`:

```java
  private Builder doExecute(
      FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan,
      List<RewriteFileGroup> groups,
      RewriteDataFilesCommitManager commitManager) {
    ...
    Tasks.Builder<RewriteFileGroup> rewriteTaskBuilder =
        Tasks.foreach(groups)
    ...
  }

  private Builder doExecuteWithPartialProgress(
      FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan,
      List<RewriteFileGroup> groups,
      RewriteDataFilesCommitManager commitManager) {
    ...
    Tasks.foreach(groups)
    ...
  }
```

Everything else (`jobDesc`, commit handling, partial-progress accounting) is unchanged; `plan` is still needed for `totalGroupCount()` and `groupsInPartition()`.

5. Delete the placeholder: `git rm --cached` is not needed because the file is untracked; simply `rm spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/PositionDeleteFilteredDataTable.java`.

- [ ] **Step 6: Format, compile and run the focused tests**

Run:
```bash
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction.testRemoveDangledEqualityDeletesPartitionEvolution" --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction.testBinPackWithV2PositionDeletes" --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction.testRewriteDataFilesPreservesLineage" --tests "org.apache.iceberg.spark.actions.TestSparkFileRewriteRunners"
```
Expected: PASS for all four parameter combinations. In the `eqDeleteJoinThreshold = 0` variants of `testRemoveDangledEqualityDeletesPartitionEvolution`, the test log must contain `Selected the equality-delete merge/join path for file group` (from `EqualityDeleteJoinPlan`) and `Rewriting file group ... with the equality-delete merge/join path` (from the runner). Check with `grep -l "merge/join path" spark/v3.5/spark/build/test-results/test/*.xml`.

- [ ] **Step 7: Run the whole compaction suite**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction"`
Expected: PASS. Every existing test now runs four times (v2/v3 × join disabled/forced). If any `threshold = 0` variant fails, fix the production code, never the assertion: these tests are the contract that the join path is observably equivalent to the reader-local path.

- [ ] **Step 8: Commit**

```bash
git add spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkDataFileRewriteRunner.java \
        spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java \
        spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkShufflingFileRewriteRunner.java \
        spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java \
        spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestRewriteDataFilesAction.java
git commit -m "Spark: Rewrite file groups with merged equality-delete joins

When eq-delete-join-threshold-records selects a file group, the action
plans cache keys once, orders reader-local groups first, and owns the
cache manager for the whole execution. The runners stage scan tasks
without equality deletes, read the rows with the _file column, filter
them with sequence-aware joins against the merged delete DataFrames,
and release the group's cache references when the rewrite ends.

TestRewriteDataFilesAction now runs every test with the join path both
disabled and forced on."
```

---
### Task 7: End-to-end correctness tests for the join path

**Files:**
- Modify: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestRewriteDataFilesAction.java`

**Interfaces:**
- Consumes: Task 6 wiring; existing helpers in the test class (`createTable()`, `createTablePartitioned(...)`, `writeDF`, `currentData()`, `currentDataFiles(table)`, `writePosDeletesToFile`, `writeDV`, `shouldHaveFiles`, `shouldHaveACleanCache`, `shouldHaveNoCachedDataFrames`, `averageFileSize`, `GroupInfoMatcher`), `FileHelpers.writeDeleteFile`, `org.apache.iceberg.TestHelpers.Row`, `EqualityDeleteTestUtil.NESTED_SCHEMA/LOCATION_TYPE/appendRows/record/struct/cityDelete/addEqualityDeletes(..., int[], Record...)` (Task 2).
- Produces: nothing new for later tasks. Every test below uses `basicRewrite(table)`, so the parameterization from Task 6 runs each scenario on both the reader-local path and the join path and both must produce identical results.

- [ ] **Step 1: Add the helpers**

Add next to `writeEqDeleteRecord`:

```java
  private void writeRecordsToOneFile(List<ThreeColumnRecord> records) {
    writeDF(spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1));
  }

  /** Writes one equality delete file keyed by {@code deleteColumn} for the current spec. */
  private DeleteFile writeEqDeletes(
      Table table, StructLike partition, String deleteColumn, Object... deleteValues)
      throws IOException {
    Schema deleteRowSchema = table.schema().select(deleteColumn);
    List<Record> deletes = Lists.newArrayList();
    for (Object value : deleteValues) {
      Record record = GenericRecord.create(deleteRowSchema);
      record.setField(deleteColumn, value);
      deletes.add(record);
    }

    OutputFile out =
        table
            .io()
            .newOutputFile(
                table
                    .locationProvider()
                    .newDataLocation(FileFormat.PARQUET.addExtension(UUID.randomUUID().toString())));
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(table, out, partition, deletes, deleteRowSchema);
    table.newRowDelta().addDeletes(deleteFile).commit();
    return deleteFile;
  }

  private List<DeleteFile> writePosDeletesOrDVs(Table table, DataFile dataFile, int positions)
      throws IOException {
    if (formatVersion >= 3) {
      return writeDV(table, dataFile.partition(), dataFile.location(), positions);
    } else {
      return writePosDeletes(table, dataFile.partition(), dataFile.location(), 1, positions);
    }
  }
```

Imports to add: `org.apache.iceberg.data.FileHelpers` and the static imports `EqualityDeleteTestUtil.partition`, `NESTED_SCHEMA`, `LOCATION_TYPE`, `appendRows`, `record`, `struct`, `cityDelete`, `addEqualityDeletes` (the class already imports `org.apache.spark.sql.Row`, so the Iceberg `Row` must not be imported directly; if a static import clashes with a method of the test class, call it as `EqualityDeleteTestUtil.xxx(...)` instead). `currentData()` sorts by `c1, c2, c3` and cannot be used for the nested schema, hence `nestedTableData()`.

- [ ] **Step 2: Add the scenario tests**

```java
  @TestTemplate
  public void testEqualityDeleteJoinKeepsRowsWrittenAfterTheDelete() throws IOException {
    Table table = createTable();
    writeRecordsToOneFile(
        Lists.newArrayList(
            new ThreeColumnRecord(1, "a", "x"),
            new ThreeColumnRecord(2, "b", "x"),
            new ThreeColumnRecord(3, "c", "x"))); // seq 1
    writeEqDeletes(table, null, "c1", 1, 2); // seq 2
    writeRecordsToOneFile(
        Lists.newArrayList(
            new ThreeColumnRecord(1, "a2", "y"), new ThreeColumnRecord(4, "d", "y"))); // seq 3
    table.refresh();
    shouldHaveFiles(table, 2);

    List<Object[]> expected = currentData();
    assertThat(expected).as("Older rows 1 and 2 are deleted, newer row 1 survives").hasSize(3);

    Result result =
        basicRewrite(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .option(RewriteDataFiles.REMOVE_DANGLING_DELETES, "true")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(2);
    assertThat(result.addedDataFilesCount()).isEqualTo(1);
    assertThat(result.removedDeleteFilesCount()).as("The delete became dangling").isEqualTo(1);
    assertEquals("Rows must match", expected, currentData());
    assertThat(TestHelpers.deleteFiles(table)).isEmpty();
    shouldHaveACleanCache(table);
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinMatchesNullKeys() throws IOException {
    Table table = createTable();
    writeRecordsToOneFile(
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "x"),
            new ThreeColumnRecord(2, "b", "x"),
            new ThreeColumnRecord(3, null, "y")));
    writeEqDeletes(table, null, "c2", (Object) null);
    table.refresh();

    List<Object[]> expected = currentData();
    assertThat(expected).hasSize(1);

    basicRewrite(table).option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true").execute();

    assertEquals("Rows with null keys must be deleted", expected, currentData());
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinAppliesEveryFieldIdSet() throws IOException {
    Table table = createTable();
    writeRecordsToOneFile(
        Lists.newArrayList(
            new ThreeColumnRecord(1, "a", "x"),
            new ThreeColumnRecord(2, "b", "x"),
            new ThreeColumnRecord(3, "c", "x"),
            new ThreeColumnRecord(4, "d", "x")));
    writeEqDeletes(table, null, "c1", 1);
    writeEqDeletes(table, null, "c2", "b");
    writeEqDeletes(table, null, "c1", 3);
    table.refresh();

    List<Object[]> expected = currentData();
    assertThat(expected).hasSize(1);

    basicRewrite(table).option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true").execute();

    assertEquals("A row deleted by any field-id set must be removed", expected, currentData());
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinWithPositionDeletes() throws IOException {
    Table table = createTable();
    writeRecordsToOneFile(
        Lists.newArrayList(
            new ThreeColumnRecord(1, "a", "x"),
            new ThreeColumnRecord(2, "b", "x"),
            new ThreeColumnRecord(3, "c", "x"),
            new ThreeColumnRecord(4, "d", "x")));
    table.refresh();
    DataFile dataFile = currentDataFiles(table).get(0);
    RowDelta rowDelta = table.newRowDelta();
    writePosDeletesOrDVs(table, dataFile, 1).forEach(rowDelta::addDeletes); // position 0
    rowDelta.commit();
    writeEqDeletes(table, null, "c1", 3);
    table.refresh();

    List<Object[]> expected = currentData();
    assertThat(expected).as("One row removed by position, one by equality").hasSize(2);

    Result result =
        basicRewrite(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .option(RewriteDataFiles.REMOVE_DANGLING_DELETES, "true")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(1);
    assertEquals("Rows must match", expected, currentData());
    assertThat(TestHelpers.deleteFiles(table)).isEmpty();
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinSort() throws IOException {
    Table table = createTable(4);
    writeEqDeletes(table, null, "c1", 1, 2, 3);
    table.refresh();
    List<Object[]> expected = currentData();

    basicRewrite(table)
        .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
        .option(
            SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES,
            Integer.toString(averageFileSize(table)))
        .sort(SortOrder.builderFor(table.schema()).asc("c2").build())
        .execute();

    assertEquals("Rows must match", expected, currentData());
    shouldHaveMultipleFiles(table);
    shouldHaveLastCommitSorted(table, "c2");
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinZOrder() throws IOException {
    Table table = createTable(4);
    writeEqDeletes(table, null, "c1", 1, 2, 3);
    table.refresh();
    List<Object[]> expected = currentData();

    basicRewrite(table)
        .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
        .option(
            SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES,
            Integer.toString(averageFileSize(table)))
        .zOrder("c2", "c3")
        .execute();

    assertEquals("Rows must match", expected, currentData());
    shouldHaveMultipleFiles(table);
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinKeepsPartitionScopeAfterPartitionEvolution()
      throws IOException {
    Table table =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation);
    writeRecordsToOneFile(Lists.newArrayList(new ThreeColumnRecord(1, "x", "p1"))); // seq 1, c1=1
    writeRecordsToOneFile(Lists.newArrayList(new ThreeColumnRecord(2, "x", "p2"))); // seq 2, c1=2
    writeEqDeletes(table, partition(1), "c2", "x"); // seq 3, scoped to partition c1=1
    table.refresh();
    table.updateSpec().addField(Expressions.ref("c3")).commit(); // both files now share one group
    table.refresh();

    List<Object[]> expected = currentData();
    assertEquals(
        "Only the row in partition c1=1 is deleted",
        ImmutableList.of(row(2, "x", "p2")),
        expected);

    Result result =
        basicRewrite(table).option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true").execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(2);
    assertEquals("The partition-scoped delete must not leak into c1=2", expected, currentData());
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinAppliesGlobalDeletesAcrossPartitions() throws IOException {
    Table table =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation);
    writeRecordsToOneFile(Lists.newArrayList(new ThreeColumnRecord(1, "x", "p1"))); // seq 1
    writeRecordsToOneFile(Lists.newArrayList(new ThreeColumnRecord(2, "x", "p2"))); // seq 2
    table.updateSpec().removeField("c1").commit(); // unpartitioned spec
    table.refresh();
    writeEqDeletes(table, null, "c2", "x"); // seq 3: global, deletes both older rows
    writeRecordsToOneFile(Lists.newArrayList(new ThreeColumnRecord(3, "x", "p3"))); // seq 4
    table.refresh();

    List<Object[]> expected = currentData();
    assertEquals("Only the newest row survives", ImmutableList.of(row(3, "x", "p3")), expected);

    Result result =
        basicRewrite(table).option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true").execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(3);
    assertEquals("Rows must match", expected, currentData());
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinWithMultipleGroupsPerPartition() throws IOException {
    Table table = createTablePartitioned(2, 4, 1000);
    shouldHaveFiles(table, 8);
    writeEqDeletes(table, partition(0, "fo"), "c3", "bar0", "bar1", "bar2");
    writeEqDeletes(table, partition(1, "fo"), "c3", "bar3");
    table.refresh();
    List<Object[]> expected = currentData();

    Result result =
        basicRewrite(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .option(
                SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES,
                Long.toString(averageFileSize(table) * 2L + 1))
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "4")
            .execute();

    assertThat(result.rewriteResults())
        .as("Each partition is split into at least two file groups sharing one delete cache")
        .hasSizeGreaterThanOrEqualTo(4);
    assertThat(result.rewrittenDataFilesCount()).isEqualTo(8);
    assertEquals("Rows must match", expected, currentData());
    shouldHaveACleanCache(table);
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinReleasesCachesWhenRewriteFails() throws IOException {
    Table table = createTable(4);
    writeEqDeletes(table, null, "c1", 1);
    table.refresh();
    List<Object[]> expected = currentData();

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .option(
                SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES,
                Long.toString(averageFileSize(table) * 2L + 1));
    RewriteDataFilesSparkAction spyRewrite = spy(realRewrite);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(new GroupInfoMatcher(1)));

    assertThatThrownBy(spyRewrite::execute)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Rewrite Failed");

    table.refresh();
    assertEquals("Table data must be unchanged", expected, currentData());
    shouldHaveACleanCache(table);
    shouldHaveNoCachedDataFrames();
  }

  @TestTemplate
  public void testEqualityDeleteJoinWithNestedEqualityField() {
    Table table =
        TABLES.create(
            NESTED_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation);
    appendRows(
        table,
        null,
        record(NESTED_SCHEMA, 1, struct(LOCATION_TYPE, "paris", 75000), "a"),
        record(NESTED_SCHEMA, 2, struct(LOCATION_TYPE, "rome", 100), "b"),
        record(NESTED_SCHEMA, 3, struct(LOCATION_TYPE, null, 200), "c"),
        record(NESTED_SCHEMA, 4, null, "d")); // seq 1
    appendRows(
        table, null, record(NESTED_SCHEMA, 5, struct(LOCATION_TYPE, "paris", 75001), "e")); // seq 2
    Schema deleteRowSchema = table.schema().select("location.city");
    // seq 3: keyed by field 3 (location.city), nested in the location struct
    addEqualityDeletes(
        table,
        null,
        deleteRowSchema,
        new int[] {3},
        cityDelete(deleteRowSchema, "paris"),
        cityDelete(deleteRowSchema, null));
    appendRows(
        table, null, record(NESTED_SCHEMA, 6, struct(LOCATION_TYPE, "paris", 75002), "f")); // seq 4
    table.refresh();
    shouldHaveFiles(table, 3);

    List<Object[]> expected = nestedTableData();
    assertEquals(
        "Reader-local path: paris rows and the null-city row older than the delete are removed",
        ImmutableList.of(
            row(2, row("rome", 100), "b"), row(4, null, "d"), row(6, row("paris", 75002), "f")),
        expected);

    Result result =
        basicRewrite(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .option(RewriteDataFiles.REMOVE_DANGLING_DELETES, "true")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(3);
    assertThat(result.addedDataFilesCount()).isEqualTo(1);
    assertThat(result.removedDeleteFilesCount()).as("The nested-key delete became dangling").isEqualTo(1);
    assertEquals("Rows must match on both delete paths", expected, nestedTableData());
    assertThat(TestHelpers.deleteFiles(table)).isEmpty();
    shouldHaveACleanCache(table);
    shouldHaveNoCachedDataFrames();
  }

  private List<Object[]> nestedTableData() {
    return rowsToJava(
        spark.read().format("iceberg").load(tableLocation).sort("id").collectAsList());
  }
```

`partition(0, "fo")` is the partition of `createTablePartitioned` (spec `identity(c1), truncate(c2, 2)`; every `c2` starts with `foo`). `createTablePartitioned(2, 4, 1000)` writes 8 files: 4 Spark partitions × 2 table partitions.

- [ ] **Step 3: Run the new tests**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction.testEqualityDeleteJoin*"`
Expected: PASS for all 11 tests × 4 parameter combinations. Both parameter values of `eqDeleteJoinThreshold` must agree with `expected`, which is computed by Spark's reader-local path before the rewrite. If the `eqDeleteJoinThreshold = -1` variant of `testEqualityDeleteJoinWithNestedEqualityField` fails on `expected`, the reader-local path is broken for nested keys on this branch: stop and report, since the join path's contract is equivalence with it.

If `testEqualityDeleteJoinKeepsPartitionScopeAfterPartitionEvolution` fails only for `eqDeleteJoinThreshold = 0` by returning zero rows, the scope join condition in `EqualityDeleteJoinFilter.applyDeletes` (Task 5) or the scope column in `EqualityDeleteScans.mergedDeletes` (Task 3) is broken; do not change the test.

- [ ] **Step 4: Run the whole suite and format**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply && ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "org.apache.iceberg.spark.actions.TestRewriteDataFilesAction"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestRewriteDataFilesAction.java
git commit -m "Spark: Test equality-delete merge/join compaction end to end

Covers sequence-number semantics, null keys, several field-id sets,
position deletes and DVs alongside equality deletes, sort and z-order
rewrites, partition-scoped deletes after partition evolution, global
deletes, shared caches across file groups, an equality key nested in a
struct, and cache cleanup after a failed rewrite. Every scenario runs on
both delete paths."
```

---

### Task 8: Procedure test and documentation

**Files:**
- Modify: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestRewriteDataFilesProcedure.java`
- Modify: `docs/docs/spark-procedures.md`

**Interfaces:**
- Consumes: option names from Task 1 (`eq-delete-join-threshold-records`, `eq-delete-join-cache-storage-level`), `FileHelpers.writeDeleteFile`, `validationCatalog.loadTable(tableIdent)` from `ExtensionsTestBase`.
- Produces: nothing for later tasks.

- [ ] **Step 1: Write the failing procedure test**

Add to `TestRewriteDataFilesProcedure`:

```java
  @TestTemplate
  public void testRewriteDataFilesWithEqualityDeleteJoin() throws IOException {
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg TBLPROPERTIES('format-version' = '2')",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a', 'x'), (2, 'b', 'x')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'c', 'y')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Schema deleteRowSchema = table.schema().select("c1");
    Record delete = GenericRecord.create(deleteRowSchema);
    delete.setField("c1", 1);
    OutputFile out =
        table
            .io()
            .newOutputFile(
                table
                    .locationProvider()
                    .newDataLocation(FileFormat.PARQUET.addExtension(UUID.randomUUID().toString())));
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(table, out, null, ImmutableList.of(delete), deleteRowSchema);
    table.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> expectedRecords = currentData();
    assertThat(expectedRecords).hasSize(2);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "options => map('rewrite-all', 'true', 'eq-delete-join-threshold-records', '0', "
                + "'eq-delete-join-cache-storage-level', 'DISK_ONLY', 'remove-dangling-deletes', 'true'))",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 2 data files and add 1 data file",
        row(2, 1),
        Arrays.copyOf(output.get(0), 2));
    assertEquals("Data after compaction should not change", expectedRecords, currentData());
    assertThat(spark.sharedState().cacheManager().isEmpty())
        .as("Merged equality-delete DataFrames must be released")
        .isTrue();
  }

  @TestTemplate
  public void testRewriteDataFilesWithInvalidEqualityDeleteJoinStorageLevel() {
    createTable();
    insertData(2);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', "
                        + "options => map('eq-delete-join-cache-storage-level', 'BOGUS'))",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse 'eq-delete-join-cache-storage-level' value BOGUS");
  }
```

Add imports: `java.io.IOException`, `java.util.UUID`, `org.apache.iceberg.DeleteFile`, `org.apache.iceberg.FileFormat`, `org.apache.iceberg.Schema`, `org.apache.iceberg.Table`, `org.apache.iceberg.data.FileHelpers`, `org.apache.iceberg.data.GenericRecord`, `org.apache.iceberg.data.Record`, `org.apache.iceberg.io.OutputFile`, `org.apache.iceberg.relocated.com.google.common.collect.ImmutableList` (skip the ones already present). If `assertThatThrownBy` reports the exception wrapped by the procedure, relax to `.hasMessageContaining(...)` on the root cause with `.rootCause()`.

- [ ] **Step 2: Run the tests**

Run: `./gradlew :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests "org.apache.iceberg.spark.extensions.TestRewriteDataFilesProcedure.testRewriteDataFilesWith*EqualityDeleteJoin*"`
Expected: PASS (the production code exists since Task 6; this task only adds coverage at the SQL surface). If the first test fails on the row count, print `output` and adjust nothing but confirm the table really has two data files before the call (`SELECT count(*) FROM %s.files`).

- [ ] **Step 3: Document the options**

In `docs/docs/spark-procedures.md`, under `rewrite_data_files` → "General Options", add after the `remove-dangling-deletes` row:

```markdown
| `eq-delete-join-threshold-records` | -1 | Maximum number of applicable equality-delete records the reader-local delete filter handles for one scan task. When a file group contains a scan task whose equality delete files reach this many records in total, the group is rewritten by joining its rows with merged equality deletes in Spark instead of loading the deletes into executor memory. Negative values disable the join path, `0` selects it for every group with equality deletes. Position deletes and deletion vectors never count. Equality fields may be top-level columns or fields nested in structs. A delete file keyed by a field nested in a list or a map, by a list or map field, or by a field that no longer exists in the table schema fails the procedure before any file group is rewritten. |
| `eq-delete-join-cache-storage-level` | MEMORY_AND_DISK | Spark storage level for merged equality-delete DataFrames that are shared by several file groups when the join path is selected. |
```

- [ ] **Step 4: Format and commit**

```bash
./gradlew :iceberg-spark:iceberg-spark-extensions-3.5_2.12:spotlessApply
git add spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestRewriteDataFilesProcedure.java \
        docs/docs/spark-procedures.md
git commit -m "Spark: Document and test equality-delete join options in rewrite_data_files

Adds procedure-level coverage for eq-delete-join-threshold-records and
eq-delete-join-cache-storage-level and documents both options."
```

---
### Task 9 (optional, spec "Add benchmarks"): JMH benchmark comparing the delete paths

Skip this task if benchmark time is not available; it does not gate the feature. It is listed so the spec's benchmark request has an owner.

**Files:**
- Create: `spark/v3.5/spark/src/jmh/java/org/apache/iceberg/spark/action/IcebergEqualityDeleteCompactionBenchmark.java`

**Interfaces:**
- Consumes: `SparkActions.get().rewriteDataFiles(table)`, options from Task 1, `GenericAppenderFactory.newEqDeleteWriter`, `OutputFileFactory`, `RandomGeneratingUDF` (already in the jmh package), setup helpers copied from `IcebergSortCompactionBenchmark` in the same directory.

- [ ] **Step 1: Write the benchmark**

```java
package org.apache.iceberg.spark.action;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
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
 * Compares reader-local equality-delete filtering with the merge/join path, with one file group
 * per partition (no shared cache) and with many file groups per partition (shared persisted cache).
 *
 * <p>Run with: {@code ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:jmh
 * -PjmhIncludeRegex=IcebergEqualityDeleteCompactionBenchmark
 * -PjmhOutputPath=benchmark/eq-delete-compaction.txt}
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
        .option(SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES, Long.toString(maxGroupSizeBytes))
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
                new RandomGeneratingUDF(NUM_ROWS).randomLongUDF().apply().cast(DataTypes.IntegerType))
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
    for (org.apache.iceberg.FileScanTask task : table.newScan().planFiles()) {
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
      return Files.createTempDirectory("benchmark-").toAbsolutePath() + "/" + UUID.randomUUID() + "/";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
```

Import `org.apache.iceberg.FileScanTask` instead of the inline FQN before saving.

- [ ] **Step 2: Compile the jmh source set and run one variant**

Run: `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:jmhCompileGeneratedClasses` then
`./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:jmh -PjmhIncludeRegex=IcebergEqualityDeleteCompactionBenchmark.readerLocal -PjmhOutputPath=benchmark/eq-delete-readerlocal.txt`
Expected: compiles; the benchmark completes and writes the output file. Record the three benchmark numbers for at least the smallest parameter combination in the commit message body.

- [ ] **Step 3: Format and commit**

```bash
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:spotlessApply
git add spark/v3.5/spark/src/jmh/java/org/apache/iceberg/spark/action/IcebergEqualityDeleteCompactionBenchmark.java
git commit -m "Spark: Benchmark reader-local vs merge/join equality-delete compaction"
```

---

## Spec Coverage Matrix

| Spec section / test-plan item | Task |
|---|---|
| Option `eq-delete-join-threshold-records`, semantics of -1 / 0 / positive | 1 (option), 2 (`useMergeJoin`), 7 (parameterized e2e) |
| Short-circuit threshold, `remaining` subtraction, position deletes ignored, no cross-task dedup | 2 (`useMergeJoin` + 8 unit tests) |
| Cache scope keyed by field-id set + exact delete file umbrella | 2 (`DeleteCacheKey`, `GroupJoinInfo.of`) |
| Partition-scoped vs global deletes cached independently | 2 (split by `spec.isUnpartitioned()`), 3 (scope column), 5 (scope join) |
| Dependency model `requiredCachesByGroup` / `remainingConsumersByCache`, `globalIndex` keys, one reference per (group, key) | 2 (`plan`), 4 (manager) |
| Build merged deletes once per key, `max(delete_sequence_number)`, persist only when reused, materialize before rewrites | 3 (`mergedDeletes`), 4 (`CacheEntry.dataFrame`) |
| Stage tasks without equality deletes, keep position deletes/DVs, read table columns + `_data_sequence_number` | 3 (`withoutEqualityDeletes`), 6 (`scanTasks`, `readGroup`), 5 (`fileAttributes` join) |
| Null-safe left-outer join + sequence predicate, per field-id set and per global umbrella | 5 |
| Release on terminal outcome only, idempotent, concurrency-safe, exactly-once unpersist, action-level `finally` cleanup | 4 (manager + tests), 6 (runner `finally`, action try-with-resources) |
| Scheduling: normal groups first, compatible groups adjacent, bounded concurrency | 2 (`orderGroups`), 6 (action) |
| Correctness rules: no helper columns written, commit path unchanged, keys by field ID, no silent fallback | 5 (drop helpers), 6 (write path untouched), 2 (`EqualityKeyPath`, plan-time validation), 3 (`keyPaths`), 3/6 (`checkArgument` failures) |
| Keys resolved by Iceberg field ID for fields nested in structs, with the reader-local null semantics; unsupported keys fail before any rewrite | 2 (`EqualityKeyPath` + expression tests, plan validation tests), 3 (nested merged deletes), 5 (nested joins + reader-local comparison), 7 (e2e on both paths), 8 (docs) |
| Test plan: below threshold uses reader path; equal to threshold selects join; accumulation; pos deletes ignored; negative/zero; stops at first task | 2 |
| Test plan: multiple groups share persisted DataFrames; split per field-id set; delete file read once | 2 (`groupsWithSameDeleteFilesShareOneCacheKey`), 4 (`sharedCachesArePersisted...`, `builds == 1`), 7 (`...MultipleGroupsPerPartition`) |
| Test plan: older row removed, newer row survives; null keys; multiple field-id sets; partitioned/unpartitioned; position + equality deletes; sort and z-order | 5 (filter tests), 7 |
| Test plan: partial cache overlap `{A,B},{A,C},{C}`; group references key from several tasks once; concurrent completion unpersists once; repeated terminal idempotent; failed final attempt releases; fail-fast cleanup | 4 (manager tests), 2 (`groupReferencingKeyFromSeveralTasksCountsOnce`), 7 (`...ReleasesCachesWhenRewriteFails`) |
| Test plan: intermediate failed attempt does not release | Not applicable: the action never retries a group (`Tasks...noRetry()`); the runner releases only when `rewrite()` exits, which is always final. If retries are added later, move `onGroupTerminal` to the retry loop. |
| Test plan: global equality deletes cached separately | 2, 5 (`globalDeletesApplyAcrossPartitions...`), 7 |
| Test plan: cache cleanup after success and failure | 6/7 (`shouldHaveNoCachedDataFrames`, `shouldHaveACleanCache`) |
| Existing e2e compaction tests pass with the join algorithm | 6 (parameterization), 7 |
| Benchmarks | 9 (optional) |
| Procedure surface and docs | 8 |

## Execution Notes for Subagents

- Work in place on branch `criteo-1.10.x`; one commit per task, no squashing across tasks.
- Before starting a task, read the spec, this plan's header sections, and the task's **Interfaces** block; then read the files listed under **Files** in the current tree. Names in later tasks are the exact names produced by earlier tasks.
- The build needs JDK 17 or 21 (`java -version`); the machine has JDK 21.
- Spotless rewrites formatting and adds license headers only for files that already have the header text; add the standard Apache header (copy from any neighbor file) to every new file yourself.
- Do not modify tests to make production code pass in Tasks 6 and 7; the join path must be observably equivalent to the reader-local path.
- If a Spark API used here does not exist on 3.5.6 (`Dataset.drop(String...)`, `Dataset.storageLevel()`, `StorageLevel.fromString`, `functions.broadcast`, `RelationalGroupedDataset` with aliased columns), check the alternative noted in the task text before redesigning.
- Run `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleMain :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleTest` before every commit in addition to spotless; spotless does not catch Javadoc line length or unused imports.
- On branch `criteo-1.10.x` the v1 form of every task is already committed (`git log -- spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/`), and later commits changed details this plan still describes in v1 form: `FILE_COLUMN` is `__rewrite_file_path` filled by `input_file_name()` rather than `_file`, partition scopes are integer IDs from a plan-wide registry (`scopeId(ContentFile)` replaced `scopeKey`, and `EqualityDeleteScans` takes the plan), the attribute joins are left-outer joins guarded by `checkedSequenceNumber()`, and a group with two tasks over one data file is rejected. When executing this plan there, treat each task as reconciling the current files with the task's code: keep those later changes, apply only the nested-key delta (Tasks 2, 3, 5, 7, 8; Tasks 1, 4, 6, 9 need nothing), and commit the delta with the task's message.
