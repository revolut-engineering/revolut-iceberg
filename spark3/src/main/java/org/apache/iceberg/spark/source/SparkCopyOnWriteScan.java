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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

class SparkCopyOnWriteScan extends SparkBatchScan implements SupportsRuntimeFiltering {

  private final Long snapshotId;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;

  // lazy variables
  private List<FileScanTask> files = null; // lazy cache of files
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks
  private Set<String> filteredLocations = null;

  SparkCopyOnWriteScan(SparkSession spark, Table table, boolean caseSensitive, Schema expectedSchema,
                       List<Expression> filters, CaseInsensitiveStringMap options) {

    super(spark, table, caseSensitive, expectedSchema, filters, options);

    Map<String, String> props = table.properties();

    long tableSplitSize = PropertyUtil.propertyAsLong(props, SPLIT_SIZE, SPLIT_SIZE_DEFAULT);
    this.splitSize = Spark3Util.propertyAsLong(options, SparkReadOptions.SPLIT_SIZE, tableSplitSize);

    int tableSplitLookback = PropertyUtil.propertyAsInt(props, SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT);
    this.splitLookback = Spark3Util.propertyAsInt(options, SparkReadOptions.LOOKBACK, tableSplitLookback);

    long tableOpenFileCost = PropertyUtil.propertyAsLong(props, SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(options, SparkReadOptions.FILE_OPEN_COST, tableOpenFileCost);

    Preconditions.checkArgument(!options.containsKey("snapshot-id"), "Cannot have snapshot-id in options");
    Snapshot currentSnapshot = table.currentSnapshot();
    this.snapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;
    // init files with an empty list if the table is empty to avoid picking any concurrent changes
    this.files = currentSnapshot == null ? Collections.emptyList() : null;
  }

  Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Statistics estimateStatistics() {
    if (snapshotId == null) {
      return new Stats(0L, 0L);
    }
    return super.estimateStatistics();
  }

  @Override
  public NamedReference[] filterAttributes() {
    return new NamedReference[] { Expressions.column(MetadataColumns.FILE_PATH.name()) };
  }

  @Override
  public void filter(Filter[] filters) {
    for (Filter filter : filters) {
      if (filter instanceof In && ((In) filter).attribute().equalsIgnoreCase(MetadataColumns.FILE_PATH.name())) {
        In in = (In) filter;

        Set<String> fileLocations = Sets.newHashSet();
        for (Object value : in.values()) {
          fileLocations.add((String) value);
        }

        // Spark may call this multiple times for UPDATEs with subqueries
        // as such cases are rewritten using UNION and the same scan on both sides
        // so filter files only if it is beneficial
        if (fileLocations.size() < filteredLocations.size()) {
          this.tasks = null;
          this.filteredLocations = fileLocations;
          this.files = files().stream()
              .filter(file -> fileLocations.contains(file.file().path().toString()))
              .collect(Collectors.toList());
        }
      }
    }
  }

  // should be accessible to the write
  synchronized List<FileScanTask> files() {
    if (files == null) {
      TableScan scan = table()
          .newScan()
          .ignoreResiduals()
          .caseSensitive(caseSensitive())
          .useSnapshot(snapshotId)
          .project(expectedSchema());

      for (Expression filter : filterExpressions()) {
        scan = scan.filter(filter);
      }

      try (CloseableIterable<FileScanTask> filesIterable = scan.planFiles()) {
        this.files = Lists.newArrayList(filesIterable);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return files;
  }

  @Override
  protected synchronized List<CombinedScanTask> tasks() {
    if (tasks == null) {
      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files()),
          splitSize);
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, splitSize,
          splitLookback, splitOpenFileCost);
      tasks = Lists.newArrayList(scanTasks);
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

    SparkCopyOnWriteScan that = (SparkCopyOnWriteScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(filteredLocations, that.filteredLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), readSchema(), filterExpressions().toString(),
        snapshotId, filteredLocations);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergCopyOnWriteScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }
}
