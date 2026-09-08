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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
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
import org.apache.iceberg.util.StructLikeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver-side plan for the equality-delete merge/join compaction path.
 *
 * <p>Decides which file groups use the merge/join path, derives the {@link DeleteCacheKey cache
 * keys} each group needs (one partition-scoped key and one global key per equality field-id set)
 * and records the many-to-many group-to-cache dependencies. Every equality field ID of the selected
 * groups is validated against the current table schema here, so an unsupported key fails the action
 * before any file group is rewritten. The dependency maps are immutable once the plan is built;
 * file groups are identified by {@code FileGroupInfo.globalIndex()}.
 *
 * <p>The plan also owns the {@link #scopeId(ContentFile) partition scope IDs} of every file it
 * selects. The IDs must come from this single plan-wide registry because a merged delete DataFrame
 * is shared by several file groups, so both sides of a partition-scoped join must agree on them.
 */
class EqualityDeleteJoinPlan {
  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteJoinPlan.class);
  private static final EqualityDeleteJoinPlan EMPTY =
      new EqualityDeleteJoinPlan(
          ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), new ScopeRegistry());

  private final Map<Integer, GroupJoinInfo> joinInfoByGroup;
  private final Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup;
  private final Map<DeleteCacheKey, Set<Integer>> consumersByCache;
  private final ScopeRegistry scopes;

  private EqualityDeleteJoinPlan(
      Map<Integer, GroupJoinInfo> joinInfoByGroup,
      Map<Integer, Set<DeleteCacheKey>> requiredCachesByGroup,
      Map<DeleteCacheKey, Set<Integer>> consumersByCache,
      ScopeRegistry scopes) {
    this.joinInfoByGroup = joinInfoByGroup;
    this.requiredCachesByGroup = requiredCachesByGroup;
    this.consumersByCache = consumersByCache;
    this.scopes = scopes;
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
    ScopeRegistry scopes = new ScopeRegistry();

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
        for (DeleteFile delete : key.deleteFiles()) {
          scopes.register(table, delete);
        }
      }

      for (DataFile dataFile : info.dataFiles()) {
        scopes.register(table, dataFile);
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
    consumers.forEach(
        (key, groupIndexes) -> immutableConsumers.put(key, ImmutableSet.copyOf(groupIndexes)));

    return new EqualityDeleteJoinPlan(
        ImmutableMap.copyOf(joinInfos),
        ImmutableMap.copyOf(requiredCaches),
        immutableConsumers.build(),
        scopes);
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
   * Partition scope of a content file: partition-scoped equality deletes apply only to files with
   * the same spec ID and partition value, which is exactly what this ID identifies.
   *
   * <p>Partitions are compared structurally, so distinct partitions never share an ID even when
   * they render to the same human-readable partition path, as a string value of {@code "null"} and
   * an actual {@code NULL} do.
   *
   * @param file a data or delete file of a group selected by this plan
   * @return the plan-wide ID of the file's partition scope
   * @throws IllegalArgumentException if the file was not registered by this plan
   */
  int scopeId(ContentFile<?> file) {
    return scopes.lookup(file);
  }

  /**
   * Assigns dense plan-wide IDs to the distinct partition scopes of the files of every selected
   * group, in first-seen order.
   *
   * <p>Only {@link #plan} registers scopes, before the plan is constructed, so the registry is
   * read-only once the plan is published and lookups from concurrent group rewrites are safe.
   */
  private static class ScopeRegistry {
    private final Map<Integer, StructLikeWrapper> wrappersBySpec = Maps.newHashMap();
    private final Map<Integer, Map<StructLikeWrapper, Integer>> idsBySpec = Maps.newHashMap();
    private int nextId = 0;

    void register(Table table, ContentFile<?> file) {
      PartitionSpec spec = table.specs().get(file.specId());
      Preconditions.checkArgument(
          spec != null,
          "Cannot find partition spec %s of file %s in table %s",
          file.specId(),
          file.location(),
          table.name());
      StructLikeWrapper wrapper =
          wrappersBySpec
              .computeIfAbsent(
                  spec.specId(), ignored -> StructLikeWrapper.forType(spec.partitionType()))
              .copyFor(file.partition());
      Map<StructLikeWrapper, Integer> ids =
          idsBySpec.computeIfAbsent(spec.specId(), ignored -> Maps.newHashMap());
      if (!ids.containsKey(wrapper)) {
        ids.put(wrapper, nextId);
        this.nextId = nextId + 1;
      }
    }

    int lookup(ContentFile<?> file) {
      StructLikeWrapper prototype = wrappersBySpec.get(file.specId());
      Map<StructLikeWrapper, Integer> ids = idsBySpec.get(file.specId());
      Integer id = prototype == null ? null : ids.get(prototype.copyFor(file.partition()));
      Preconditions.checkArgument(
          id != null,
          "Cannot find the partition scope of file %s: spec %s and partition %s are not part of the equality-delete join plan",
          file.location(),
          file.specId(),
          file.partition());
      return id;
    }
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
      Set<String> dataFileLocations = Sets.newHashSet();
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
        // the join reads each data file once, so a second task over the same file would duplicate
        // every row of that file in the attribute join
        Preconditions.checkArgument(
            dataFileLocations.add(file.location()),
            "Cannot use the equality-delete join path: file group has more than one task for data file %s",
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
