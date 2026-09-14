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
        deleteFiles.stream().collect(Collectors.toCollection(DeleteFileSet::create)).stream()
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
