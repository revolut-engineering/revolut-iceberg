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

package org.apache.iceberg.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Locale;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class CoreMetricsUtil {

  private CoreMetricsUtil() {
  }

  private static final MetricRegistry metricRegistry = new MetricRegistry();

  public static MetricRegistry metricRegistry() {
    return metricRegistry;
  }

  public static Timer timer(String type, String metric) {
    return metricRegistry.timer(MetricRegistry.name(type, metric));
  }

  public static TableOperations wrapWithMeterIfConfigured(Configuration conf, TableOperationsMetricType type,
      TableOperations delegate) {
    return wrapWithMeterIfConfigured(
        conf,
        delegate,
        t -> new MeteredTableOperations(metricRegistry, type, t));
  }

  private static <T> T wrapWithMeterIfConfigured(Configuration conf, T delegate, Function<T, T> wrapperFunc) {
    Preconditions.checkArgument(conf != null, "Configuration is null");

    if (conf.getBoolean("iceberg.dropwizard.enable-metrics-collection", false)) {
      return wrapperFunc.apply(delegate);
    } else {
      return delegate;
    }
  }

  public enum TableOperationsMetricType {
    HIVE("hive"),
    HADOOP("hadoop"),
    STATIC("static");

    private final String prefix;

    public String prefix() {
      return prefix;
    }

    TableOperationsMetricType(String prefix) {
      this.prefix = prefix;
    }

    public static TableOperationsMetricType from(String type) {
      Preconditions.checkArgument(type != null, "TableOperations Metric Type is null");
      return TableOperationsMetricType.valueOf(type.toUpperCase(Locale.ENGLISH));
    }
  }
}
