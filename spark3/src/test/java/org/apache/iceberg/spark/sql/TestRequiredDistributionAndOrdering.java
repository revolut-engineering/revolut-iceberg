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

package org.apache.iceberg.spark.sql;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

// TODO: temporarily ignore until Spark supports Iceberg transforms
@Ignore
public class TestRequiredDistributionAndOrdering extends SparkCatalogTestBase {
  public TestRequiredDistributionAndOrdering(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testGlobalSortOnBucketedColumn() throws NoSuchTableException {
    sql("CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) " +
        "USING iceberg " +
        "PARTITIONED BY (bucket(2, c1))", tableName);

    List<ThreeColumnRecord> data = ImmutableList.of(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(3, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(4, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(5, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(6, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(7, "BBBBBBBBBB", "BBBB")
    );
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should succeed by default
    inputDF.writeTo(tableName).append();

    // should automatically correct the ordering
    sql("ALTER TABLE %s WRITE ORDERED BY (c1, c2, c3)", tableName);
    inputDF.writeTo(tableName).append();

    // should succeed with correct global sort
    sql("ALTER TABLE %s WRITE ORDERED BY (bucket(2, c1), c2, c3)", tableName);
    inputDF.writeTo(tableName).append();

    // should fail if ordering is disabled
    AssertHelpers.assertThrows("Should reject writes without ordering",
        SparkException.class, "Writing job aborted",
        () -> {
          try {
            inputDF.writeTo(tableName)
                .option(SparkWriteOptions.DISTRIBUTION_MODE, "none")
                .option(SparkWriteOptions.IGNORE_SORT_ORDER, "true")
                .append();
          } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testDefaultSortOnDecimalBucketedColumn() {
    sql("CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) " +
        "USING iceberg " +
        "PARTITIONED BY (bucket(2, c2))", tableName);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2)", tableName);

    List<Object[]> expected = ImmutableList.of(
        row(1, new BigDecimal("20.20")),
        row(2, new BigDecimal("40.20"))
    );

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testDefaultSortOnStringBucketedColumn() {
    sql("CREATE TABLE %s (c1 INT, c2 STRING) " +
        "USING iceberg " +
        "PARTITIONED BY (bucket(2, c2))", tableName);

    sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

    List<Object[]> expected = ImmutableList.of(
        row(1, "A"),
        row(2, "B")
    );

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testGlobalSortOnTruncatedColumn() throws NoSuchTableException {
    sql("CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) " +
        "USING iceberg " +
        "PARTITIONED BY (truncate(4, c2))", tableName);

    List<ThreeColumnRecord> data = ImmutableList.of(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(3, "AAAAAAAAAA", "BBBB"),
        new ThreeColumnRecord(4, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(5, "AAAAAAAAAA", "BBBB"),
        new ThreeColumnRecord(6, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(7, "AAAAAAAAAA", "BBBB")
    );
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should succeed by default
    inputDF.writeTo(tableName).append();

    // should automatically correct the ordering
    sql("ALTER TABLE %s WRITE ORDERED BY (c1, c2, c3)", tableName);
    inputDF.writeTo(tableName).append();

    // should succeed with global sort
    sql("ALTER TABLE %s WRITE ORDERED BY (truncate(4, c2), c2, c3)", tableName);
    inputDF.writeTo(tableName).append();

    // should fail if ordering is disabled
    AssertHelpers.assertThrows("Should reject writes without ordering",
        SparkException.class, "Writing job aborted",
        () -> {
          try {
            inputDF.writeTo(tableName)
                .option(SparkWriteOptions.DISTRIBUTION_MODE, "none")
                .option(SparkWriteOptions.IGNORE_SORT_ORDER, "true")
                .append();
          } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testDefaultSortOnDecimalTruncatedColumn() {
    sql("CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) " +
        "USING iceberg " +
        "PARTITIONED BY (truncate(2, c2))", tableName);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2)", tableName);

    List<Object[]> expected = ImmutableList.of(
        row(1, new BigDecimal("20.20")),
        row(2, new BigDecimal("40.20"))
    );

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testDefaultSortOnLongTruncatedColumn() {
    sql("CREATE TABLE %s (c1 INT, c2 BIGINT) " +
        "USING iceberg " +
        "PARTITIONED BY (truncate(2, c2))", tableName);

    sql("INSERT INTO %s VALUES (1, 22222222222222), (2, 444444444444)", tableName);

    List<Object[]> expected = ImmutableList.of(
        row(1, 22222222222222L),
        row(2, 444444444444L)
    );

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }
}
