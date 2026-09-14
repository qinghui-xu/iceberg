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
 * Column path of an equality field in the table schema and the Spark expression that projects a row
 * onto that field.
 *
 * <p>Equality deletes identify their key columns by field ID, so a key may be a field nested in
 * structs. The reader-local path ({@code DeleteFilter}) compares the {@code StructProjection} of
 * each row onto the key fields: a row whose parent struct is null is not equal to a row whose
 * parent struct is present with a null leaf, while two null parents are equal. The join path must
 * agree, so {@link #column} builds, for every ancestor from the leaf upwards, {@code CASE WHEN
 * parent IS NOT NULL THEN struct(child) END}, which is null for a null parent. The result is a
 * struct-typed key whose null-safe equality is exactly the structural equality of the Iceberg
 * projection. A top-level field's expression is the column itself.
 *
 * <p>Fields nested in a list or a map, and fields that are lists or maps, are not supported:
 * Iceberg cannot project a partial list element or map entry, and Spark cannot group or join on map
 * values. {@link #rejectionReason} reports these cases so callers can fail with context.
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
