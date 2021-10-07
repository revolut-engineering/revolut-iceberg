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

package org.apache.iceberg.spark.metrics;

import com.codahale.metrics.MetricRegistry;
import org.apache.iceberg.hive.metrics.HiveMetricsUtil;
import org.apache.iceberg.metrics.CoreMetricsUtil;
import org.apache.spark.metrics.source.Source;

public class IcebergMetricSource implements Source {

  private static final String sourceName = "iceberg";
  private final MetricRegistry metricRegistry = new MetricRegistry();

  public IcebergMetricSource() {
    metricRegistry.register("core", CoreMetricsUtil.metricRegistry());
    metricRegistry.register("hms", HiveMetricsUtil.metricRegistry());
    metricRegistry.register("query", SparkMetricsUtil.metricRegistry());
  }

  @Override
  public String sourceName() {
    return sourceName;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
