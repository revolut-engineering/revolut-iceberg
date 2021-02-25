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

package org.apache.iceberg.spark.extensions;

import java.util.Map;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDistributedAndOrderedTables extends SparkExtensionsTestBase {

  public TestDistributedAndOrderedTables(String catalogName, String implementation,
                                         Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testCreateGloballyOrderedTable() {
    sql("CREATE TABLE %s (id INT, data STRING) " +
        "USING iceberg " +
        "ORDERED BY (id, data)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "range", distributionMode);

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .asc("id", NullOrder.NULLS_FIRST)
        .asc("data", NullOrder.NULLS_FIRST)
        .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testCreateAsSelectLocallyOrderedTable() {
    sql("CREATE TABLE %s " +
        "USING iceberg " +
        "LOCALLY ORDERED BY (col1, col2)" +
        "AS SELECT * FROM VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "none", distributionMode);

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .asc("col1", NullOrder.NULLS_FIRST)
        .asc("col2", NullOrder.NULLS_FIRST)
        .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY col1 ASC NULLS LAST", tableName));
  }

  @Test
  public void testReplaceHashDistributedTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    assertEquals("Table should be empty",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("REPLACE TABLE %s (id INT, data STRING) " +
        "USING iceberg " +
        "PARTITIONED BY (data) " +
        "DISTRIBUTED BY PARTITION " +
        "ORDERED BY id", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .asc("id", NullOrder.NULLS_FIRST)
        .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testReplaceAsSelectOrderedTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    assertEquals("Table should be empty",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("REPLACE TABLE %s " +
        "USING iceberg " +
        "PARTITIONED BY (col2) " +
        "ORDERED BY (col1) " +
        "AS SELECT * FROM VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "range", distributionMode);

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .asc("col1", NullOrder.NULLS_FIRST)
        .build();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY col1 ASC NULLS LAST", tableName));
  }

  @Test
  public void testReplaceAsSelectUnorderedTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    assertEquals("Table should be empty",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("REPLACE TABLE %s " +
        "USING iceberg " +
        "PARTITIONED BY (col2) " +
        "UNORDERED " +
        "AS SELECT * FROM VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "none", distributionMode);

    SortOrder expected = SortOrder.unsorted();
    Assert.assertEquals("Should have expected order", expected, table.sortOrder());

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY col1 ASC NULLS LAST", tableName));
  }
}
