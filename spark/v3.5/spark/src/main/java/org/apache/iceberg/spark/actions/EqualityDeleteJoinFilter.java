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
                dataRows
                    .col(EqualityDeleteScans.FILE_COLUMN)
                    .equalTo(attributes.col(EqualityDeleteScans.LOCATION_COLUMN)),
                "inner")
            .drop(EqualityDeleteScans.LOCATION_COLUMN);

    for (DeleteCacheKey key : info.partitionScopedKeys()) {
      rows = applyDeletes(rows, key);
    }

    for (DeleteCacheKey key : info.globalKeys()) {
      rows = applyDeletes(rows, key);
    }

    return rows.drop(
        EqualityDeleteScans.FILE_COLUMN,
        EqualityDeleteScans.SEQUENCE_NUMBER_COLUMN,
        EqualityDeleteScans.SCOPE_COLUMN);
  }

  private Dataset<Row> applyDeletes(Dataset<Row> rows, DeleteCacheKey key) {
    Dataset<Row> deletes = caches.mergedDeletes(key);
    List<EqualityKeyPath> keyPaths = scans.keyPaths(key.equalityFieldIds());

    Column condition = null;
    for (int pos = 0; pos < keyPaths.size(); pos++) {
      Column match =
          keyPaths
              .get(pos)
              .column(rows::col)
              .eqNullSafe(deletes.col(EqualityDeleteScans.DELETE_KEY_PREFIX + pos));
      condition = condition == null ? match : condition.and(match);
    }

    if (key.partitionScoped()) {
      condition =
          condition.and(
              rows.col(EqualityDeleteScans.SCOPE_COLUMN)
                  .equalTo(deletes.col(EqualityDeleteScans.DELETE_SCOPE_COLUMN)));
    }

    Column latestDeleteSequenceNumber =
        deletes.col(EqualityDeleteScans.DELETE_SEQUENCE_NUMBER_COLUMN);
    Column survives =
        latestDeleteSequenceNumber
            .isNull()
            .or(
                rows.col(EqualityDeleteScans.SEQUENCE_NUMBER_COLUMN)
                    .geq(latestDeleteSequenceNumber));

    return rows.join(deletes, condition, "left_outer").filter(survives).drop(deletes.columns());
  }
}
