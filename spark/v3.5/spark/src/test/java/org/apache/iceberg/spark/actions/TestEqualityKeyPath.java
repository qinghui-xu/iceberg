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
            row(1, row("paris"), row((Object) row("fr"))),
            row(2, row((Object) null), row((Object) row("fr"))),
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
    String query =
        "SELECT * FROM VALUES "
            + "(1, named_struct('city', CAST(NULL AS STRING))), "
            + "(2, CAST(NULL AS STRUCT<city: STRING>)), "
            + "(3, named_struct('city', CAST(NULL AS STRING))), "
            + "(4, CAST(NULL AS STRUCT<city: STRING>)) "
            + "AS t(id, location)";
    Dataset<Row> rows = spark.sql(query);
    EqualityKeyPath city = EqualityKeyPath.of(schema, 3);

    Dataset<Row> grouped = rows.groupBy(city.column(rows::col).as("key")).count().sort("key");
    assertEquals(
        "Null parents group together and apart from present parents with a null leaf",
        ImmutableList.of(row(null, 2L), row(row((Object) null), 2L)),
        rowsToJava(grouped.collectAsList()));

    // the join path always combines two independently built row sets, never one with itself
    Dataset<Row> left = rows.alias("l");
    Dataset<Row> right = spark.sql(query).alias("r");
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
