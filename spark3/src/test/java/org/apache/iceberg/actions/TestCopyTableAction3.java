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

package org.apache.iceberg.actions;

import java.io.File;
import java.util.Map;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.junit.Test;

public class TestCopyTableAction3 extends TestCopyTableAction {
  @Override
  protected ActionsProvider actions() {
    return SparkActions.get();
  }

  @Test
  public void testMetadataLocationChange() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createMetastoreTable(sourceTableLocation, Maps.newHashMap(), "tbl", 1);
    String metadataFilePath = currentMetadata(sourceTable).metadataFileLocation();

    String newMetadataDir = "new-metadata-dir";
    sourceTable.updateProperties().set(
        TableProperties.WRITE_METADATA_LOCATION,
        sourceTableLocation + newMetadataDir).commit();

    spark.sql("insert into hive.default.tbl values (1, 'AAAAAAAAAA', 'AAAA')");
    sourceTable.refresh();

    // copy table
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .execute();

    checkMetadataFileNum(4, 2, 2, result);
    checkDataFileNum(2, result);

    // pick up a version from the old metadata dir as the end version
    CopyTable.Result result1 = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .endVersion(fileName(metadataFilePath))
        .execute();

    checkMetadataFileNum(2, 1, 1, result1);

    // pick up a version from the old metadata dir as the last copied version
    CopyTable.Result result2 = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .lastCopiedVersion(fileName(metadataFilePath))
        .execute();

    checkMetadataFileNum(2, 1, 1, result2);
  }

  @Test
  public void testMetadataCompressionWithMetastoreTable() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    Table sourceTable = createMetastoreTable(sourceTableLocation, properties, "testMetadataCompression", 2);

    TableMetadata currentMetadata = currentMetadata(sourceTable);

    // set the second version as the endVersion
    String endVersion = fileName(currentMetadata.previousFiles().get(1).file());
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .endVersion(endVersion)
        .execute();

    checkMetadataFileNum(4, result);
    checkDataFileNum(1, result);

    // set the first version as the lastCopiedVersion
    String firstVersion = fileName(currentMetadata.previousFiles().get(0).file());
    result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .lastCopiedVersion(firstVersion)
        .execute();

    checkMetadataFileNum(6, result);
    checkDataFileNum(2, result);
  }

  private TableMetadata currentMetadata(Table tbl) {
    return ((HasTableOperations) tbl).operations().current();
  }

  private Table createMetastoreTable(
      String tableLocation, Map<String, String> properties, String tableName,
      int snapshotNumber) {
    spark.conf().set("spark.sql.catalog.hive", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.hive.type", "hive");
    spark.conf().set("spark.sql.catalog.hive.default-namespace", "default");

    StringBuilder propertiesStr = new StringBuilder();
    properties.forEach((k, v) -> propertiesStr.append("'" + k + "'='" + v + "',"));
    String tblProperties = propertiesStr.substring(0, propertiesStr.length() > 0 ? propertiesStr.length() - 1 : 0);

    if (tblProperties.isEmpty()) {
      sql("CREATE TABLE hive.default.%s (c1 bigint, c2 string, c3 string) USING iceberg LOCATION '%s'",
          tableName, tableLocation);
    } else {
      sql("CREATE TABLE hive.default.%s (c1 bigint, c2 string, c3 string) USING iceberg LOCATION '%s' TBLPROPERTIES " +
          "(%s)", tableName, tableLocation, tblProperties);
    }

    for (int i = 0; i < snapshotNumber; i++) {
      sql("insert into hive.default.%s values (1, 'AAAAAAAAAA', 'AAAA')", tableName);
    }
    return catalog.loadTable(TableIdentifier.of("default", tableName));
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }
}
