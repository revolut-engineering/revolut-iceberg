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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestCopyTableAction extends SparkTestBase {
  protected abstract ActionsProvider actions();

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  protected String tableLocation = null;
  private Table table = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
    this.table = createATableWith2Snapshots(tableLocation);
  }

  private Table createATableWith2Snapshots(String location) {
    return createTableWithSnapshots(location, 2);
  }

  private Table createTableWithSnapshots(String location, int snapshotNumber) {
    return createTableWithSnapshots(location, snapshotNumber, Maps.newHashMap());
  }

  protected Table createTableWithSnapshots(String location, int snapshotNumber, Map<String, String> properties) {
    Table newTable = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, location);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    for (int i = 0; i < snapshotNumber; i++) {
      df.select("c1", "c2", "c3")
          .write()
          .format("iceberg")
          .mode("append")
          .save(location);
    }

    return newTable;
  }

  @Test
  public void testCopyTable() throws Exception {
    String targetTableLocation = newTableLocation();

    // check the data file location before the rebuild
    List<String> validDataFiles = spark.read().format("iceberg")
        .load(tableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
    Assert.assertEquals("Should be 2 valid data files", 2, validDataFiles.size());

    CopyTable.Result result = actions().copyTable(table)
        .rewriteLocationPrefix(tableLocation, targetTableLocation)
        .endVersion("v3.metadata.json")
        .execute();

    Assert.assertEquals("The latest version should be", "v3.metadata.json", result.latestVersion());

    checkMetadataFileNum(3, 2, 2, result);
    checkDataFileNum(2, result);

    // copy the metadata files and data files
    moveTableFiles(tableLocation, targetTableLocation, stagingDir(result));

    // verify the data file path after the rebuild
    List<String> validDataFilesAfterRebuilt = spark.read().format("iceberg")
        .load(targetTableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
    Assert.assertEquals("Should be 2 valid data files", 2, validDataFilesAfterRebuilt.size());
    for (String item : validDataFilesAfterRebuilt) {
      Assert.assertTrue("Data file should point to the new location", item.startsWith(targetTableLocation));
    }

    // verify data rows
    Dataset<Row> resultDF = spark.read().format("iceberg").load(targetTableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testDataFilesDiff() throws Exception {
    CopyTable.Result result = actions().copyTable(table)
        .rewriteLocationPrefix(tableLocation, newTableLocation())
        .lastCopiedVersion("v2.metadata.json")
        .execute();

    checkDataFileNum(1, result);

    List<String> rebuiltFiles =
        spark.read().format("text").load(result.metadataFileListLocation()).as(Encoders.STRING()).collectAsList();

    // v3.metadata.json, one manifest-list file, one manifest file
    checkMetadataFileNum(3, result);

    String currentSnapshotId = String.valueOf(table.currentSnapshot().snapshotId());
    Assert.assertTrue(
        "Should have the current snapshot file",
        rebuiltFiles.stream().filter(c -> c.contains(currentSnapshotId)).count() == 1);

    String parentSnapshotId = String.valueOf(table.currentSnapshot().parentId());
    Assert.assertTrue(
        "Should NOT have the parent snapshot file",
        rebuiltFiles.stream().filter(c -> c.contains(parentSnapshotId)).count() == 0);
  }

  @Test
  public void testTableWith3Snapshots() throws Exception {
    String location = newTableLocation();
    Table tableWith3Snaps = createTableWithSnapshots(location, 3);
    CopyTable.Result result = actions().copyTable(tableWith3Snaps)
        .rewriteLocationPrefix(location, newTableLocation())
        .lastCopiedVersion("v2.metadata.json")
        .execute();

    checkMetadataFileNum(2, 2, 2, result);
    checkDataFileNum(2, result);

    // start from the first version
    CopyTable.Result result1 = actions().copyTable(tableWith3Snaps)
        .rewriteLocationPrefix(location, newTableLocation())
        .lastCopiedVersion("v1.metadata.json")
        .execute();

    checkMetadataFileNum(3, 3, 3, result1);
    checkDataFileNum(3, result1);
  }

  @Test
  public void testFullTableCopy() throws Exception {
    CopyTable.Result result = actions().copyTable(table)
        .rewriteLocationPrefix(tableLocation, newTableLocation())
        .execute();

    checkMetadataFileNum(3, 2, 2, result);
    checkDataFileNum(2, result);
  }

  @Test
  public void testDeleteDataFile() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(location);
    List<String> validDataFiles = spark.read().format("iceberg")
        .load(location + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();

    sourceTable.newDelete().deleteFile(validDataFiles.stream().findFirst().get()).commit();

    String targetLocation = newTableLocation();
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(location, targetLocation)
        .execute();

    checkMetadataFileNum(4, 3, 3, result);
    checkDataFileNum(2, result);

    // copy the metadata files and data files
    moveTableFiles(location, targetLocation, stagingDir(result));

    // verify data rows
    Dataset<Row> resultDF = spark.read().format("iceberg").load(targetLocation);
    Assert.assertEquals("There are only one row left since we deleted a data file", 1, resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .count());
  }

  @Test
  public void testFullTableCopyWithDeletedVersionFiles() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createTableWithSnapshots(location, 2);
    // expire the first snapshot
    Table staticTable = newStaticTable(location + "metadata/v2.metadata.json", table.io());
    Actions.forTable(sourceTable)
        .expireSnapshots()
        .expireSnapshotId(staticTable.currentSnapshot().snapshotId())
        .execute();

    // create 100 more snapshots
    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    for (int i = 0; i < 100; i++) {
      df.select("c1", "c2", "c3")
          .write()
          .format("iceberg")
          .mode("append")
          .save(location);
    }
    sourceTable.refresh();

    // v1/v2/v3.metadata.json has been deleted in v104.metadata.json, and there is no way to find the first snapshot
    // from the version file history
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(location, newTableLocation())
        .execute();

    // you can only find 101 snapshots but the manifest file and data file count should be 102.
    checkMetadataFileNum(101, 101, 102, result);
    checkDataFileNum(102, result);
  }

  protected Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  @Test
  public void testRewriteTableWithoutSnapshot() throws Exception {
    CopyTable.Result result = actions().copyTable(table)
        .rewriteLocationPrefix(tableLocation, newTableLocation())
        .endVersion("v1.metadata.json")
        .execute();

    // the only rebuilt file is v1.metadata.json since it contains no snapshot
    checkMetadataFileNum(1, result);
    checkDataFileNum(0, result);
  }

  @Test
  public void testExpireSnapshotBeforeRewrite() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    Actions.forTable(sourceTable)
        .expireSnapshots()
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .execute();

    checkMetadataFileNum(4, 1, 2, result);

    checkDataFileNum(2, result);
  }

  @Test
  public void testStartSnapshotWithoutValidSnapshot() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    Actions.forTable(sourceTable)
        .expireSnapshots()
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    Assert.assertEquals("1 out 2 snapshot has been removed", 1, ((List) sourceTable.snapshots()).size());

    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .lastCopiedVersion("v2.metadata.json")
        .execute();

    // 2 metadata.json, 1 manifest list file, 1 manifest files
    checkMetadataFileNum(4, result);
    checkDataFileNum(1, result);
  }

  @Test
  public void testMoveTheVersionExpireSnapshot() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    Actions.forTable(sourceTable)
        .expireSnapshots()
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    // only move version v4, which is the version generated by snapshot expiration
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .lastCopiedVersion("v3.metadata.json")
        .execute();

    // only v4.metadata.json needs to move
    checkMetadataFileNum(1, result);
    // no data file needs to move
    checkDataFileNum(0, result);
  }

  @Test
  public void testMoveVersionWithInvalidSnapshots() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    Actions.forTable(sourceTable)
        .expireSnapshots()
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    AssertHelpers.assertThrows(
        "Copy a version with invalid snapshots aren't allowed",
        UnsupportedOperationException.class,
        () -> actions().copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .endVersion("v3.metadata.json")
            .execute());
  }

  @Test
  public void testRollBack() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);
    Long secondSnapshotId = sourceTable.currentSnapshot().snapshotId();

    // roll back to the first snapshot(v2)
    sourceTable.rollback().toSnapshotId(sourceTable.currentSnapshot().parentId()).commit();

    // add a new snapshot
    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(sourceTableLocation);

    sourceTable.refresh();

    // roll back to the second snapshot(v3)
    sourceTable.rollback().toSnapshotId(secondSnapshotId).commit();

    // copy table
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .execute();

    // check the result
    checkMetadataFileNum(6, 3, 3, result);
  }

  @Test
  public void testWriteAuditPublish() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // enable WAP
    sourceTable.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
    spark.conf().set("spark.wap.id", "1");

    // add a new snapshot without changing the current snapshot of the table
    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(sourceTableLocation);

    sourceTable.refresh();

    // copy table
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .execute();

    // check the result. There are 3 snapshots in total, although the current snapshot is the second one.
    checkMetadataFileNum(5, 3, 3, result);
  }

  @Test
  public void testSchemaChange() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // change the schema
    sourceTable.updateSchema().addColumn("c4", Types.StringType.get()).commit();

    // copy table
    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .execute();

    // check the result
    checkMetadataFileNum(4, 2, 2, result);
  }

  @Test
  public void testWithTargetTable() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 3);
    String targetTableLocation = newTableLocation();
    Table targetTable = createATableWith2Snapshots(targetTableLocation);

    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
        .targetTable(targetTable)
        .execute();

    Assert.assertEquals("The latest version should be", "v4.metadata.json", result.latestVersion());

    // 3 files rebuilt from v3 to v4: v4.metadata.json, one manifest list, one manifest file
    checkMetadataFileNum(3, result);
  }

  @Test
  public void testInvalidStartVersion() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 3);
    String targetTableLocation = newTableLocation();
    Table targetTable = createATableWith2Snapshots(targetTableLocation);

    AssertHelpers.assertThrows("The valid start version should be v3", IllegalArgumentException.class,
        "The start version isn't the current version of the target table.",
        () -> actions().copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .targetTable(targetTable)
            .lastCopiedVersion("v2.metadata.json")
            .execute());
  }

  @Test
  public void testPmeEnabled() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("parquet.encryption.footer.key", "keyA");

    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 1, properties);

    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .execute();

    checkDataFileNum(2, result);

    List<String> dataFilesToMove  =
        spark.read().format("text").load(result.dataFileListLocation()).as(Encoders.STRING()).collectAsList();
    Assert.assertEquals("1 key material file should be moved", 1,
        dataFilesToMove.stream().filter(f -> f.contains("_KEY_MATERIAL_FOR_")).count());
  }

  @Test
  public void testSnapshotIdInheritanceEnabled() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true");

    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 2, properties);

    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .execute();

    checkMetadataFileNum(7, result);
    checkDataFileNum(2, result);
  }

  @Test
  public void testMetadataCompression() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 2, properties);

    CopyTable.Result result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .endVersion("v2.gz.metadata.json")
        .execute();

    checkMetadataFileNum(4, result);
    checkDataFileNum(1, result);

    result = actions().copyTable(sourceTable)
        .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
        .lastCopiedVersion("v1.gz.metadata.json")
        .execute();

    checkMetadataFileNum(6, result);
    checkDataFileNum(2, result);
  }

  @Test
  public void testInvalidArgs() {
    CopyTable actions = actions().copyTable(table);

    AssertHelpers.assertThrows("", IllegalArgumentException.class, "Source prefix('') cannot be empty",
        () -> actions.rewriteLocationPrefix("", null));

    AssertHelpers.assertThrows("", IllegalArgumentException.class, "Source prefix('null') cannot be empty",
        () -> actions.rewriteLocationPrefix(null, null));

    AssertHelpers.assertThrows("", IllegalArgumentException.class, "Staging location('') cannot be empty",
        () -> actions.stagingLocation(""));

    AssertHelpers.assertThrows("", IllegalArgumentException.class, "Staging location('null') cannot be empty",
        () -> actions.stagingLocation(null));

    AssertHelpers.assertThrows("", IllegalArgumentException.class, "Last copied version('null') cannot be empty",
        () -> actions.lastCopiedVersion(null));

    AssertHelpers.assertThrows("Last copied version cannot be empty", IllegalArgumentException.class,
        () -> actions.lastCopiedVersion(" "));

    AssertHelpers.assertThrows("End version cannot be empty", IllegalArgumentException.class,
        () -> actions.endVersion(" "));

    AssertHelpers.assertThrows("End version cannot be empty", IllegalArgumentException.class,
        () -> actions.endVersion(null));
  }

  protected void checkDataFileNum(long count, CopyTable.Result result) {
    List<String> filesToMove  =
        spark.read().format("text").load(result.dataFileListLocation()).as(Encoders.STRING()).collectAsList();
    Assert.assertEquals("The rebuilt data file number should be", count, filesToMove.size());
  }

  protected void checkMetadataFileNum(int count, CopyTable.Result result) {
    List<String> filesToMove  =
        spark.read().format("text").load(result.metadataFileListLocation()).as(Encoders.STRING()).collectAsList();
    Assert.assertEquals("The rebuilt metadata file number should be", count, filesToMove.size());
  }

  protected void checkMetadataFileNum(
      int versionFileCount, int manifestListCount,
      int manifestFileCount, CopyTable.Result result) {
    List<String> filesToMove  =
            spark.read().format("text").load(result.metadataFileListLocation()).as(Encoders.STRING()).collectAsList();
    Assert.assertEquals("The rebuilt version file number should be", versionFileCount,
        filesToMove.stream().filter(f -> f.endsWith(".metadata.json")).count());
    Assert.assertEquals("The rebuilt Manifest list file number should be", manifestListCount,
        filesToMove.stream().filter(f -> f.contains("snap-")).count());
    Assert.assertEquals("The rebuilt Manifest file number should be", manifestFileCount,
        filesToMove.stream().filter(f -> f.endsWith("-m0.avro")).count());
  }

  private String stagingDir(CopyTable.Result result) {
    String metadataFileListPath = result.metadataFileListLocation();
    return metadataFileListPath.substring(0, metadataFileListPath.lastIndexOf(File.separator));
  }

  protected String newTableLocation() throws IOException {
    return temp.newFolder().toURI().toString();
  }

  private void moveTableFiles(String sourceDir, String targetDir, String stagingDir) throws Exception {
    FileUtils.copyDirectory(new File(removePrefix(sourceDir) + "data/"), new File(removePrefix(targetDir) + "/data/"));
    FileUtils.copyDirectory(new File(removePrefix(stagingDir)), new File(removePrefix(targetDir) + "/metadata/"));
  }

  private String removePrefix(String path) {
    return path.substring(path.lastIndexOf(":") + 1);
  }
}
