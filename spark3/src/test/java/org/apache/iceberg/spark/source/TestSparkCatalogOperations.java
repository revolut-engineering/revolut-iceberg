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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestSparkCatalogOperations extends SparkCatalogTestBase {

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        { "testhive", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default"
            ) },
        { "spark_catalog", SparkSessionCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default",
                "parquet-enabled", "true",
                "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
            ) }
    };
  }

  public TestSparkCatalogOperations(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  String tblName;
  TableIdentifier tblIdentifier;

  @Before
  public void initTables() {
    tblIdentifier = TableIdentifier.of(Namespace.of("default"), "table" + System.currentTimeMillis());
    tblName = (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default.table" +
        System.currentTimeMillis();
  }

  @Test
  public void testPurge() throws IOException {
    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tblName);
    Table table = validationCatalog.loadTable(tblIdentifier);
    Path path = new Path(table.location());
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tblName);
    sql("DROP TABLE %s PURGE", tblName);
    FileSystem fs = Util.getFs(path, spark.sessionState().newHadoopConf());
    Assert.assertFalse("All files should be purged", fs.listFiles(path, true).hasNext());
  }

  @Test
  public void testPurgeWhenGCDisabled() throws IOException {
    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tblName);
    Table table = validationCatalog.loadTable(tblIdentifier);
    String location = table.location();
    table.updateProperties()
        .set(TableProperties.GC_ENABLED, "false")
        .commit();
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tblName);
    sql("DROP TABLE %s PURGE", tblName);

    Path path = new Path(location);
    FileSystem fs = Util.getFs(path, spark.sessionState().newHadoopConf());
    Assert.assertTrue("Files should not be deleted when GC is enabled", fs.listFiles(path, true).hasNext());
  }
}
