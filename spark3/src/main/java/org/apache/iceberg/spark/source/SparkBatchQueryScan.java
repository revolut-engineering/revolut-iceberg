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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.metrics.SparkMetricsUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkBatchQueryScan extends SparkBatchScan implements SupportsRuntimeFiltering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchQueryScan.class);

  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;
  private final List<Expression> evaluatedRuntimeFilterExprs;

  private Set<Integer> specIds = null; // lazy cache of scanned spec IDs
  private List<FileScanTask> files = null; // lazy cache of files
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  SparkBatchQueryScan(SparkSession spark, Table table, SparkReadConf readConf, boolean caseSensitive,
                      Schema expectedSchema, List<Expression> filters, CaseInsensitiveStringMap options) {

    super(spark, table, readConf, caseSensitive, expectedSchema, filters, options);

    this.snapshotId = readConf.snapshotId();
    this.asOfTimestamp = readConf.asOfTimestamp();

    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    if (snapshotId != null || asOfTimestamp != null) {
      if (startSnapshotId != null || endSnapshotId != null) {
        throw new IllegalArgumentException(
            "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either " +
                SparkReadOptions.SNAPSHOT_ID + " or " + SparkReadOptions.AS_OF_TIMESTAMP + " is specified");
      }
    } else if (startSnapshotId == null && endSnapshotId != null) {
      throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
    }

    this.splitSize = table instanceof BaseMetadataTable ? readConf.metadataSplitSize() : readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();
    this.evaluatedRuntimeFilterExprs = Lists.newArrayList();
  }

  private Set<Integer> specIds() {
    if (specIds == null) {
      Set<Integer> specIdSet = Sets.newHashSet();
      for (FileScanTask file : files()) {
        specIdSet.add(file.spec().specId());
      }
      this.specIds = specIdSet;
    }

    return specIds;
  }

  private List<FileScanTask> files() {
    if (files == null) {
      TableScan scan = table()
          .newScan()
          .option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize))
          .option(TableProperties.SPLIT_LOOKBACK, String.valueOf(splitLookback))
          .option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(splitOpenFileCost))
          .caseSensitive(caseSensitive())
          .project(expectedSchema());

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (startSnapshotId != null) {
        if (endSnapshotId != null) {
          scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
        } else {
          scan = scan.appendsAfter(startSnapshotId);
        }
      }

      for (Expression filter : filterExpressions()) {
        scan = scan.filter(filter);
      }

      try (CloseableIterable<FileScanTask> filesIterable = scan.planFiles()) {
        this.files = Lists.newArrayList(filesIterable);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close table scan: " + scan, e);
      }
    }

    return files;
  }

  @Override
  protected List<CombinedScanTask> tasks() {
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
  public NamedReference[] filterAttributes() {
    Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

    for (Integer specId : specIds()) {
      PartitionSpec spec = table().specs().get(specId);
      for (PartitionField field : spec.fields()) {
        partitionFieldSourceIds.add(field.sourceId());
      }
    }

    // TODO: support source field names with dots

    return partitionFieldSourceIds.stream()
        .map(fieldId -> expectedSchema().findColumnName(fieldId))
        .filter(Objects::nonNull)
        .map(org.apache.spark.sql.connector.expressions.Expressions::column)
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(Filter[] filters) {
    Expression runtimeFilterExpr = Expressions.alwaysTrue();

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        try {
          Binder.bind(expectedSchema().asStruct(), expr, caseSensitive());
          runtimeFilterExpr = Expressions.and(runtimeFilterExpr, expr);
        } catch (ValidationException e) {
          LOG.error("Failed to bind {} to expected schema, skipping runtime filter", expr, e);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", filter);
      }
    }

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (Integer specId : specIds()) {
        Expression inclusiveExpr = Projections.inclusive(table().spec()).project(runtimeFilterExpr);
        Evaluator inclusive = new Evaluator(table().spec().partitionType(), inclusiveExpr);
        evaluatorsBySpecId.put(specId, inclusive);
      }

      LOG.info("Trying to filter {} files using runtime filter {}", files().size(), runtimeFilterExpr);

      List<FileScanTask> filteredFiles = files().stream()
          .filter(file -> {
            Evaluator evaluator = evaluatorsBySpecId.get(file.spec().specId());
            return evaluator.eval(file.file().partition());
          })
          .collect(Collectors.toList());

      LOG.info("{} files matched runtime filter {}", filteredFiles.size(), runtimeFilterExpr);

      if (filteredFiles.size() < files().size()) {
        this.specIds = null;
        this.files = filteredFiles;
        this.tasks = null;
      }

      evaluatedRuntimeFilterExprs.add(runtimeFilterExpr);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        evaluatedRuntimeFilterExprs.toString().equals(that.evaluatedRuntimeFilterExprs.toString()) &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(startSnapshotId, that.startSnapshotId) &&
        Objects.equals(endSnapshotId, that.endSnapshotId) &&
        Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), readSchema(), filterExpressions().toString(), evaluatedRuntimeFilterExprs.toString(),
        snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, runtimeFilters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), evaluatedRuntimeFilterExprs, caseSensitive());
  }

  @SuppressWarnings("checkstyle:RegexpSingleline")
  static SparkBatchQueryScan create(SparkSession spark, Table table, SparkReadConf readConf, boolean caseSensitive,
                                    Schema expectedSchema, List<Expression> filters, CaseInsensitiveStringMap options) {
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    Preconditions.checkArgument(conf != null, "Configuration is null");
    if (conf.getBoolean("iceberg.dropwizard.enable-metrics-collection", false)) {
      MetricRegistry metricRegistry = SparkMetricsUtil.metricRegistry();
      return new MeteredSparkBatchQueryScan(metricRegistry, spark, table, readConf, caseSensitive, expectedSchema,
              filters, options);
    } else {
      return new SparkBatchQueryScan(spark, table, readConf, caseSensitive, expectedSchema, filters, options);
    }
  }

  private static class MeteredSparkBatchQueryScan extends SparkBatchQueryScan {
    private final MetricRegistry metricRegistry;

    private static final String QUERY_PLAN_TIME = "query.plan.time";

    MeteredSparkBatchQueryScan(MetricRegistry metricRegistry, SparkSession spark, Table table,
                               SparkReadConf readConf, boolean caseSensitive, Schema expectedSchema,
                               List<Expression> filters, CaseInsensitiveStringMap options) {
      super(spark, table, readConf, caseSensitive, expectedSchema, filters, options);
      this.metricRegistry = metricRegistry;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      try (Timer.Context ctx = metricRegistry.timer(QUERY_PLAN_TIME).time()) {
        return super.planInputPartitions();
      }
    }
  }
}
