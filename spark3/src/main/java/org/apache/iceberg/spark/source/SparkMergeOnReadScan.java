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
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

// TODO: should also support dynamic partition filtering (through a common parent?)
class SparkMergeOnReadScan extends SparkBatchScan {

  private final Long snapshotId;
  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;

  private List<CombinedScanTask> tasks;

  SparkMergeOnReadScan(SparkSession spark, Table table, SparkReadConf readConf,
                       boolean caseSensitive, Schema expectedSchema,
                       List<Expression> filters, CaseInsensitiveStringMap options) {
    super(spark, table, readConf, caseSensitive, expectedSchema, filters, options);

    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();

    Preconditions.checkArgument(!options.containsKey(SparkReadOptions.SNAPSHOT_ID), "Can't set snapshot-id in options");
    Snapshot currentSnapshot = table.currentSnapshot();
    this.snapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;

    // init tasks with an empty list if the table is empty to avoid picking any concurrent changes
    this.tasks = currentSnapshot == null ? Collections.emptyList() : null;
  }

  Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Statistics estimateStatistics() {
    Snapshot snapshot = snapshotId != null ? table().snapshot(snapshotId) : null;
    return estimateStatistics(snapshot);
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table()
          .newScan()
          .option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize))
          .option(TableProperties.SPLIT_LOOKBACK, String.valueOf(splitLookback))
          .option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(splitOpenFileCost))
          .caseSensitive(caseSensitive())
          .project(expectedSchema())
          .useSnapshot(snapshotId);

      for (Expression filter : filterExpressions()) {
        scan = scan.filter(filter);
      }

      try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
        this.tasks = Lists.newArrayList(tasksIterable);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close table scan: " + scan, e);
      }
    }

    return tasks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkMergeOnReadScan that = (SparkMergeOnReadScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        Objects.equals(snapshotId, that.snapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table().name(), readSchema(), filterExpressions().toString(), snapshotId);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergMergeOnReadScan(table=%s, type=%s, filters=%s, caseSensitive%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }
}
