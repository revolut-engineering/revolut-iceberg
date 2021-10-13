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

import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.types.StructType;

import static org.apache.iceberg.MetadataColumns.FILE_PATH;
import static org.apache.iceberg.MetadataColumns.PARTITION_COLUMN_NAME;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.MetadataColumns.SPEC_ID;

class SparkPositionDeltaWriteBuilder implements DeltaWriteBuilder {

  private final SparkSession spark;
  private final Table table;
  private final Command command;
  private final SparkMergeOnReadScan scan;
  private final IsolationLevel isolationLevel;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo info;
  private final boolean handleTimestampWithoutZone;
  private final boolean checkNullability;
  private final boolean checkOrdering;
  private final DistributionMode distributionMode;

  SparkPositionDeltaWriteBuilder(SparkSession spark, Table table, Command command, Scan scan,
                                 IsolationLevel isolationLevel, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.command = command;
    this.scan = (SparkMergeOnReadScan) scan;
    this.isolationLevel = isolationLevel;
    this.writeConf = new SparkWriteConf(spark, table, info.options());
    this.info = info;
    this.handleTimestampWithoutZone = writeConf.handleTimestampWithoutZone();
    this.checkNullability = writeConf.checkNullability();
    this.checkOrdering = writeConf.checkOrdering();
    this.distributionMode = Spark3Util.distributionModeFor(table, info.options());
  }

  @Override
  public DeltaWrite build() {
    Preconditions.checkArgument(handleTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(table.schema()),
        SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);

    Schema dataSchema = dataSchema();
    if (dataSchema != null) {
      TypeUtil.validateWriteSchema(table.schema(), dataSchema, checkNullability, checkOrdering);
    }

    Schema expectedRowIdSchema = new Schema(FILE_PATH, ROW_POSITION);
    Schema rowIdSchema = SparkSchemaUtil.convert(expectedRowIdSchema, info.rowIdSchema());
    TypeUtil.validateSchema("row ID", expectedRowIdSchema, rowIdSchema, checkNullability, checkOrdering);

    Schema expectedMetadataSchema = new Schema(SPEC_ID, MetadataColumns.metadataColumn(table, PARTITION_COLUMN_NAME));
    Schema metadataSchema = SparkSchemaUtil.convert(expectedMetadataSchema, info.metadataSchema());
    TypeUtil.validateSchema("metadata", expectedMetadataSchema, metadataSchema, checkNullability, checkOrdering);

    SparkUtil.validatePartitionTransforms(table.spec());

    Distribution distribution = Spark3Util.buildPositionDeltaDistribution(distributionMode, command, table);
    SortOrder[] ordering = Spark3Util.buildPositionDeltaRequiredOrdering(distribution, command, table);

    return new SparkPositionDeltaWrite(
        spark, table, command, scan, isolationLevel, writeConf,
        info, dataSchema, distribution, ordering);
  }

  private Schema dataSchema() {
    StructType dataSparkType = info.schema();
    return dataSparkType != null ? SparkSchemaUtil.convert(table.schema(), dataSparkType) : null;
  }
}
