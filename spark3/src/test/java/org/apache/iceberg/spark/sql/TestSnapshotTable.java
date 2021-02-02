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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSnapshotTable extends SparkCatalogTestBase {
  private static final String sourceName = "spark_catalog.default.source";

  public TestSnapshotTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %S", sourceName);
  }

  @Test
  public void testSnapshot() throws IOException {
    String location = temp.newFolder().toString();
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", sourceName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    sql("SNAPSHOT TABLE %s AS %s", sourceName, tableName);

    Table createdTable = validationCatalog.loadTable(tableIdent);
    String tableLocation = createdTable.location();
    Assert.assertNotEquals("Table should not have the original location", location, tableLocation);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSnapshotWithProperties() throws IOException {
    String location = temp.newFolder().toString();
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", sourceName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    sql("SNAPSHOT TABLE %s AS %s TBLPROPERTIES ('foo' 'bar')", sourceName, tableName);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    String tableLocation = createdTable.location();
    Assert.assertNotEquals("Table should not have the original location", location, tableLocation);

    Map<String, String> props = createdTable.properties();
    Assert.assertEquals("Should have extra property set", "bar", props.get("foo"));

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSnapshotWithAlternateLocation() throws IOException {
    Assume.assumeTrue("No Snapshoting with Alternate locations with Hadoop Catalogs", !catalogName.contains("hadoop"));
    String location = temp.newFolder().toString();
    String snapshotLocation = temp.newFolder().toString();
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", sourceName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    sql("SNAPSHOT TABLE %s AS %s LOCATION '%s'", sourceName, tableName, snapshotLocation);

    String storageLocation = validationCatalog.loadTable(tableIdent).location();
    Assert.assertEquals("Snapshot should be made at specified location", snapshotLocation, storageLocation);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testForbidAlternateProvider() throws IOException {
    Assume.assumeTrue("No Snapshoting with Alternate locations with Hadoop Catalogs", !catalogName.contains("hadoop"));
    String location = temp.newFolder().toString();
    String snapshotLocation = temp.newFolder().toString();
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", sourceName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    AssertHelpers.assertThrows(
        "Should forbid using any format but Iceberg",
        IllegalArgumentException.class,
        () -> sql("SNAPSHOT TABLE %s AS %s.%s USING parquet LOCATION '%s'",
            sourceName, catalogName, tableName, snapshotLocation)
    );
  }
}
