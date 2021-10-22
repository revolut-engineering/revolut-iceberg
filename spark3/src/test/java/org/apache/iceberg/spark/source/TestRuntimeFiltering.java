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

package org.apache.iceberg.spark.source;

import java.util.Map;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Test;

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

public class TestRuntimeFiltering extends SparkCatalogTestBase {

  public TestRuntimeFiltering(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS dim");
  }

  @Test
  public void testIdentityPartitionedTable() throws NoSuchTableException {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (date)", tableName);

    for (int batchNum = 0; batchNum < 4; batchNum++) {
      Dataset<Row> df = spark.range(100)
          .withColumn("date", date_add(current_date(), batchNum))
          .withColumn("ts", expr("TO_TIMESTAMP(date)"))
          .withColumn("data", expr("CAST(date AS STRING)"))
          .select("id", "data", "date", "ts");

      df.coalesce(1).writeTo(tableName).append();
    }

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF = spark.range(10)
        .withColumn("date", current_date())
        .select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    assertEquals("Should have expected rows",
        sql("SELECT * FROM %s WHERE date = CURRENT_DATE() ORDER BY id", tableName),
        sql("SELECT f.* FROM %s f JOIN dim d ON f.date = d.date AND d.id = 1 ORDER BY id", tableName));
  }

  @Test
  public void testBucketedTable() throws NoSuchTableException {
    sql("CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) " +
        "USING iceberg " +
        "PARTITIONED BY (bucket(8, id))", tableName);

    for (int batchNum = 0; batchNum < 4; batchNum++) {
      Dataset<Row> df = spark.range(100)
          .withColumn("date", date_add(current_date(), batchNum))
          .withColumn("ts", expr("TO_TIMESTAMP(date)"))
          .withColumn("data", expr("CAST(date AS STRING)"))
          .select("id", "data", "date", "ts");

      df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();
    }

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF = spark.range(1, 2)
        .withColumn("date", current_date())
        .select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    assertEquals("Should have expected rows",
        sql("SELECT * FROM %s WHERE id = 1 ORDER BY date", tableName),
        sql("SELECT f.* FROM %s f JOIN dim d ON f.id = d.id AND d.date = CURRENT_DATE() ORDER BY date", tableName));
  }
}
