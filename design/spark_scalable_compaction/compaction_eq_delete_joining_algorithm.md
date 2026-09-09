# Threshold-Aware Cached Sequence-Aware Equality Delete Handling for Spark Data Compaction

## Problem

`RewriteDataFilesSparkAction` rewrites data file groups by reading each group's
planned `FileScanTask`s through the Iceberg Spark reader and writing replacement
data files. The current reader path applies equality deletes by loading
applicable delete rows into executor memory. That is efficient for moderate
delete sets, but it can OOM executors when a hot partition, or an unpartitioned
table, accumulates many equality deletes.

The sequence-aware union algorithm improves the large-delete path by reading the
distinct active equality delete files in a rewrite group once, merging them by
key, and filtering data rows with:

```text
data_file.data_sequence_number < equality_delete_file.data_sequence_number
```

This version retains the partition-scoped cache planning layer and defines how
each file group selects between reader-local equality-delete filtering and the
merge/join path. When multiple merge/join file groups in the same partition need
the same umbrella set of equality deletes, the action should build merged
equality-delete DataFrames once, persist them, compact all file groups that use
them, then unpersist them.

This version also defines cache ownership when a file group needs multiple
equality-delete DataFrames and groups share only part of their cache
dependencies. Cache reuse and release are decided independently for each cache
key rather than for a whole file-group batch.

## Key Idea

Use partition-scoped, field-id-set-scoped delete caches.

Within one partition rewrite scope, partition-scoped equality deletes cannot
match rows in another partition because those other rows are not in the cache
scope. That means the persisted merged delete DataFrames do not need a
`partition_domain` join column.

For each partition scope and equality field-id set:

```text
mergedDeletes[fieldIds] =
    deleteRows
    .groupBy(equality key columns)
    .agg(max(delete_sequence_number).as(latest_delete_sequence_number))
    .persist()
```

Each data file group in that partition reuses the matching cached
`mergedDeletes[fieldIds]` DataFrame and filters rows with null-safe key equality
plus the sequence predicate.

Global equality deletes are handled separately because they are not
partition-scoped.

## Cache Scope

A cache scope is the unit that owns persisted equality-delete DataFrames.

For a partitioned table:

```text
PartitionDeleteCacheKey = {
  partition,
  equalityFieldIds,
  umbrellaDeleteFileSetForFieldIds
}
```

For an unpartitioned table:

```text
TableDeleteCacheKey = {
  table,
  equalityFieldIds,
  umbrellaDeleteFileSetForFieldIds
}
```

The `umbrellaDeleteFileSetForFieldIds` is the union of equality delete files for
one equality field-id set required by the file groups that are scheduled
together. It should be built from planned `FileScanTask.deletes()` lists, not
rediscovered from metadata tables.

The cache should be created only when reuse is expected. For example, persist a
merged delete DataFrame only if at least two file groups in the same partition
scope need the same field-id-set umbrella, or if the estimated delete
read/aggregation cost is high enough to justify caching.

## Merge/Join Selection Threshold

Add an action option:

`eq-delete-join-threshold-records`

The option controls the maximum applicable equality-delete record count that the
reader-local path should handle for one `FileScanTask`.

Recommended behavior:

- Default: `-1`, which leaves the new merge/join path disabled unless the user
  opts in.
- Negative value: disable the merge/join path and always use the reader-local
  path.
- `0`: use the merge/join path for any file group that has an applicable
  equality delete file.
- Positive value: use the merge/join path when any `FileScanTask` has an
  applicable equality-delete record count greater than or equal to the
  threshold.

For one `FileScanTask`, calculate the applicable equality-delete record count
as:

```text
taskEqDeleteRecords =
    sum(delete.recordCount)
    for each equality delete file in task.deletes()
```

Count equality delete files from all equality field-id sets because the
reader-local path retains a delete set for each field-id set while reading the
task. Ignore position delete files and deletion vectors. Do not deduplicate
delete files across different tasks because the decision estimates the memory
pressure of each task independently.

The group decision is logically:

```text
useMergeJoin =
    any(taskEqDeleteRecords >= threshold
        for task in group.fileScanTasks())
```

The implementation should not calculate and retain the exact maximum. Inspect
tasks one at a time and select the merge/join path immediately when a task
reaches the threshold:

```text
if threshold < 0:
    use reader-local path

for each task in group.fileScanTasks():
    remaining = threshold

    for each equality delete file in task.deletes():
        if threshold == 0:
            use merge/join path

        if delete.recordCount >= remaining:
            use merge/join path

        remaining -= delete.recordCount

use reader-local path
```

Comparing against the remaining threshold avoids overflow when a task has many
delete records. This decision is a driver-side inspection of metadata already
present in the planned `FileScanTask` objects. It does not require a Spark job
or reading delete-file contents. The best case short-circuits on the first
qualifying task; the worst case visits all delete references in the group.

Record count deliberately does not account for equality-key width. It keeps the
first implementation simple and predictable. Users should choose the threshold
based on the table's equality-key types and the executor memory available per
concurrent Spark task. A future version may add a field-size-aware memory
estimate without changing the merge/join algorithm.

## Adaptive Rewrite Flow

### 1. Plan File Groups Normally

Use the existing rewrite planner to produce `RewriteFileGroup`s. Evaluate each
group using the short-circuit record-count threshold. Keep the current
reader-local path when no task reaches the threshold.

For groups selected for the merge/join path, collect:

- partition value, or the unpartitioned-table marker;
- active equality delete files from each group's planned `FileScanTask.deletes()`;
- equality field-id sets used by those delete files;
- data file sequence numbers from `FileScanTask.file().dataSequenceNumber()`.

### 2. Regroup Large-Delete File Groups by Cache Scope

Within each partition, identify reusable delete caches by equality field-id set.
Each file group may reference multiple cache keys if it has equality deletes
with multiple field-id sets.

A conservative cache key is:

```text
PartitionDeleteCacheKey = {
  partition,
  equalityFieldIds,
  exact union of active equality delete file locations for equalityFieldIds
}
```

All file groups that reference the same cache key can share the same persisted
merged delete DataFrame for that equality field-id set. A file group with
multiple equality field-id sets joins with multiple cached DataFrames.

If exact union grouping creates too many small caches, a later optimization can
use a superset umbrella within the same partition and equality field-id set.
That remains correct because equality key matching and sequence comparison
prevent false deletes, but it may increase shuffle work.

#### File Group-to-Cache Dependency Model

Represent cache use as a many-to-many dependency graph. Do not require two file
groups to have identical complete cache requirements before they can share one
of their equality-delete DataFrames.

```text
FileGroupKey = RewriteFileGroup.info().globalIndex()

requiredCachesByGroup: FileGroupKey -> set<DeleteCacheKey>
remainingConsumersByCache: DeleteCacheKey -> set<FileGroupKey>
```

`FileGroupInfo.globalIndex()` is assigned by the rewrite planner and is unique
within one rewrite plan, so it is available while cache dependencies are being
planned and remains stable throughout that action. Do not use
`partitionIndex()` by itself because it is only unique within a partition.

This `FileGroupKey` is not the random UUID created inside
`SparkDataFileRewriteRunner.rewrite()`. That UUID is a temporary rewrite
execution ID used by `SparkTableCache`, `ScanTaskSetManager`, and
`FileRewriteCoordinator`. It is created only when execution starts and may
change between attempts, so it is not suitable for planned cache ownership.

Build both maps from all file groups selected for the merge/join path before
starting those rewrites. A file group occurs once in the consumer set for each
cache key it needs, regardless of how many of its scan tasks or delete files
reference that key. Reader-local groups do not reference these caches.

For example:

```text
group-1 -> {cache-A, cache-B}
group-2 -> {cache-A, cache-C}
group-3 -> {cache-C}

cache-A remaining consumers = {group-1, group-2}
cache-B remaining consumers = {group-1}
cache-C remaining consumers = {group-2, group-3}
```

`group-1` and `group-2` can reuse `cache-A` even though their other cache
requirements differ. When `group-1` finishes, it releases both `cache-A` and
`cache-B`. `cache-B` then has no remaining consumer and can be unpersisted,
while `cache-A` must remain for `group-2`. Each cache key therefore has an
independent lifetime.

The cache entry is the indivisible reuse unit. Two groups with overlapping but
non-identical delete file sets for the same equality field-id set have different
exact-set cache keys and do not share a cache in the conservative design. To
share that overlap, planning must assign both groups to one explicit superset
umbrella cache. The same remaining-consumer algorithm then applies to that
umbrella key.

The dependency sets must be immutable once execution begins. Scheduling order
may change, but no new consumer may be added after a cache has become eligible
for release.

### 3. Build Persisted Merged Delete DataFrames

For each `PartitionDeleteCacheKey`, read each distinct partition-scoped equality
delete file in the field-id-set umbrella once.

Build one merged delete DataFrame for that key:

```text
partitionMergedDeletes[cacheKey] =
    deleteRowsForFieldIds
    .select(equality key columns, delete_sequence_number)
    .groupBy(equality key columns)
    .agg(max(delete_sequence_number).as(latest_delete_sequence_number))
    .persist(storageLevel)
```

Materialize the persisted DataFrame before starting file group rewrites, for
example with `count()`, so failures happen before replacement files are written.

Use one persisted DataFrame per cache key. Do not combine different field-id
sets into one wide relation because their key schemas and join conditions differ.

### 4. Build Persisted Global Delete DataFrames

Global equality deletes are not partition-scoped. They can be cached separately
per equality field-id set and global delete umbrella:

```text
globalMergedDeletes[globalCacheKey] =
    globalDeleteRowsForFieldIds
    .select(equality key columns, delete_sequence_number)
    .groupBy(equality key columns)
    .agg(max(delete_sequence_number).as(latest_delete_sequence_number))
    .persist(storageLevel)
```

These DataFrames can be reused across partition batches when the planned global
delete file set is identical. Keep their lifecycle independent from
partition-scoped delete caches.

### 5. Rewrite Each File Group Using Cached Deletes

For each file group in the batch, stage a modified scan task set:

- keep position deletes and deletion vectors;
- remove equality deletes from the scan tasks;
- read table output columns;
- read any auxiliary equality key columns needed for joins;
- attach `_data_sequence_number` from the source data file.

For each equality field-id set needed by the file group, join with the cached
partition-scoped merged deletes:

```text
matched =
    dataRows.join(
        partitionMergedDeletes[fieldIds],
        all equality key columns are null-safe equal,
        "left_outer")

survivors =
    matched.filter(
        latest_delete_sequence_number IS NULL
          OR _data_sequence_number >= latest_delete_sequence_number)
```

Apply global merged deletes in the same way, using
`globalMergedDeletes[fieldIds]`.

A row is written only if it survives all equality field-id-set joins and all
global-delete joins.

### 6. Write and Release Caches

Pass surviving rows to the existing bin-pack, sort, or z-order writer logic.

Treat a file group as a consumer of every cache key in
`requiredCachesByGroup[fileGroupKey]` until its rewrite reaches a terminal
outcome:

- success means the synchronous Spark write has returned and replacement files
  have been produced;
- failure means the group's final rewrite attempt has failed and will not be
  retried within the action.

The cache does not need to remain through the later Iceberg commit because the
commit uses the produced file metadata, not the cached DataFrame. Do not release
a cache when the joined DataFrame is constructed because Spark transformations
are lazy. If retries are added, do not release on a failed intermediate attempt.

On a terminal outcome, atomically remove the group from the dependency map and
release one reference from every cache key in that group's dependency set:

```text
onGroupTerminal(fileGroupKey):
    cacheKeys = requiredCachesByGroup.remove(fileGroupKey)
    if cacheKeys is null:
        return  // already released

    for cacheKey in cacheKeys:
        entry = cacheEntries.get(cacheKey)

        if entry.release(fileGroupKey) reports LAST_CONSUMER
            and cacheEntries.remove(cacheKey, entry):
            entry.dataFrame.unpersist(blocking = false)
```

`entry.release(fileGroupKey)` removes the global file-group index from the
entry's remaining consumer set and reports `LAST_CONSUMER` only for the
transition to an empty set. The operation must be concurrency-safe because file
groups may finish on different rewrite threads. Removing
`requiredCachesByGroup[fileGroupKey]` first makes the overall group release
idempotent. Atomically removing the expected cache entry ensures only one thread
calls `unpersist` for the empty transition.

Tracking remaining group IDs is preferred to a bare reference count because it
prevents double release and makes leaked references diagnosable. An atomic count
is also correct if it is paired with an exactly-once release guard per group.

If cache creation is lazy, there must still be only one build per cache key.
Concurrent consumers should share the same cache entry or build future. A cache
must not be built after its remaining-consumer set has already become empty.

The action must also close the cache manager in a top-level `finally` path and
unpersist every entry still present. This handles fail-fast execution where
planned groups never start, cancellation, cache-build failure, and unexpected
exceptions. Per-key last-consumer release reduces the normal cache lifetime;
the final cleanup prevents leaks.

Unpersist global merged delete DataFrames when all batches that reference them
have completed. Use the same remaining-consumer mechanism, but calculate their
consumer sets across every partition batch that can reference the global cache.

## Correctness Rules

- Active equality delete files must come from planned `FileScanTask.deletes()`
  lists.
- Equality deletes must be removed from the data reader path in join mode to
  avoid applying them twice.
- Position deletes and deletion vectors must still be applied by the normal
  reader before equality-delete joins.
- Partition-scoped caches must not be reused across partitions unless a
  partition/domain column is added back to the cached DataFrame and join
  condition.
- Within a partition-scoped cache, no `partition_domain` join column is needed.
- Merged delete DataFrames must be split per equality field-id set and cache
  umbrella.
- Cache sharing is per cache key. File groups do not need identical full cache
  dependency sets to reuse an individual cached DataFrame.
- Cache dependency maps must identify a file group by its plan-scoped
  `FileGroupInfo.globalIndex()`, not by `partitionIndex()` or the runner's
  temporary rewrite UUID.
- Cache references must be tracked once per file group and cache key, not once
  per scan task, delete file, or join.
- A file group must retain all of its cache references until its rewrite Spark
  action succeeds or its final attempt fails.
- A cached DataFrame may be unpersisted early only when its planned remaining
  consumer set becomes empty.
- Cache release must be concurrency-safe and idempotent, and action-level
  cleanup must unpersist any entries that remain after abnormal termination.
- Each merged delete DataFrame must use `max(delete_sequence_number)` per
  equality key.
- Equality key columns must be resolved by Iceberg field ID.
- Equality key comparisons must be null-safe.
- A data row is deleted only when
  `_data_sequence_number < latest_delete_sequence_number`.
- Global equality deletes and partition-scoped equality deletes must be applied
  independently.
- Internal columns and auxiliary equality key columns must not be written to
  replacement data files.
- The rewrite commit path and replacement file sequence-number assignment remain
  unchanged.

## Performance Analysis

Let:

- `G` be the number of large-delete file groups in one partition batch.
- `N_g` be data rows read by file group `g`.
- `D_f` be equality delete rows in the cache umbrella for field-id set `f`.
- `U_f` be merged delete keys for equality field-id set `f`.
- `F` be the number of equality field-id sets.

Without cache sharing, the sequence-aware per-group path may read and aggregate
the same active equality delete files once per file group:

```text
delete read and aggregate cost ~= sum over groups and field-id sets of D_f
```

With partition-scoped merged delete caches:

```text
delete read and aggregate cost ~= sum over distinct cache keys of D_f
data read cost = sum(N_g)
join cost = sum over groups and field-id sets of join(N_g, U_f)
```

The cache sharing is most useful when:

- a hot partition is split into many compaction file groups;
- those groups share recent equality delete files;
- equality delete files are large enough that read and aggregation cost matters;
- the merged delete-key table is smaller than the raw delete rows or is reused
  enough times to offset persistence cost.

It may not help when:

- only one file group uses the delete set;
- delete sets are small and the normal reader path is faster;
- each file group has a very different equality delete umbrella;
- persisted delete DataFrames are too large for available memory/disk;
- shuffle skew is dominated by a few hot equality keys.

## Scheduling

The rewrite action should process cache batches in an order that maximizes reuse
while preserving existing partial-progress behavior.

Recommended scheduling:

1. Execute normal file groups with the existing path.
2. For large-delete groups, identify partition-scoped delete cache keys per
   equality field-id set.
3. Process compatible file groups close together:
   - build needed persisted merged delete DataFrames per cache key;
   - compact file groups with bounded concurrency;
   - mark each file group terminal after its final rewrite attempt;
   - release each of that group's cache references independently;
   - unpersist a partition-scoped DataFrame when its remaining-consumer set
     becomes empty.
4. Keep global delete caches alive only while referenced by a remaining file
   group in any batch.

Bound concurrency by both existing `max-concurrent-file-group-rewrites` and cache
memory pressure. Running too many file groups against the same cached DataFrame
can still overload shuffle or cache storage.

## Optional Partition-Domain Mode

If future planning wants to share one persisted delete DataFrame across multiple
partitions, add a `partition_domain` column back to the merged delete DataFrame:

```text
mergedDeletes[fieldIds] =
    deleteRows
    .groupBy(partition_domain, equality key columns)
    .agg(max(delete_sequence_number).as(latest_delete_sequence_number))
```

Then join on both `partition_domain` and equality keys. This is an optional
cache-broadening mode, not the default design. The default design keeps caches
partition-scoped and avoids carrying partition domain through the join.

## Failure Handling

Materialize cached merged delete DataFrames before writing replacement data
files. If building or materializing a cache fails, fail the corresponding file
groups before any rewrite output is committed.

If a file group rewrite fails after cache materialization, use existing rewrite
cleanup and partial-progress behavior. Always unpersist cache DataFrames in a
`finally` path for the batch.

Do not silently fall back to reader-local equality-delete filtering after the
threshold selects the join path; that may reintroduce executor OOM.

## Test Plan

Add Spark action tests that cover:

- Groups below the threshold use the existing reader path.
- A task whose equality-delete record count equals the threshold selects the
  merge/join path.
- Record counts are accumulated across multiple equality delete files and
  equality field-id sets within one task.
- Position deletes and deletion vectors do not contribute to the threshold.
- A negative threshold disables the merge/join path, while zero selects it for
  any group with applicable equality deletes.
- Threshold evaluation stops after the first qualifying task.
- Multiple file groups in one partition share persisted merged delete DataFrames.
- Persisted DataFrames are split per equality field-id set.
- The same recent equality delete file is read once for a cache batch.
- A row older than the latest matching delete sequence is removed.
- A row written after the latest matching delete sequence survives.
- Null equality-key values.
- Multiple equality field-id sets in one partition batch.
- Partial cache overlap such as groups requiring `{A, B}`, `{A, C}`, and `{C}`.
  Verify that each cache is released immediately after its own last consumer,
  independent of the other caches used by those groups.
- A group references the same cache from multiple scan tasks, but contributes
  only one cache reference.
- Concurrent group completion unpersists each cache exactly once.
- Repeated terminal notification for a group is idempotent.
- A failed final rewrite attempt releases the group's references.
- An intermediate failed attempt does not release references when a retry will
  occur.
- Fail-fast execution and cache-build failure clean up caches whose planned
  consumers never start or never reach their normal terminal callback.
- Global equality deletes cached separately from partition-scoped deletes.
- Partitioned and unpartitioned tables.
- Position deletes plus equality deletes in the same rewrite group.
- Sort and z-order rewrites, not only bin-pack rewrites.
- Cache cleanup after successful rewrites and after failures.

Add benchmarks comparing:

- current reader-local filtering;
- per-file-group sequence-aware union joins;
- partition-scoped cached sequence-aware joins.

Vary:

- number of file groups per hot partition;
- equality delete files per partition;
- delete rows per file;
- equality field-id set count;
- key duplication rate;
- cache storage level;
- rewrite concurrency.
