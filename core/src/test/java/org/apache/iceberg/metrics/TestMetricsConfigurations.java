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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.metrics.CoreMetricsUtil.TableOperationsMetricType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestMetricsConfigurations {
  Configuration conf;
  TableOperations mockTableOps;

  @Before
  public void startBefore() {
    conf = new Configuration();
    mockTableOps = mock(TableOperations.class);
  }

  @Test
  public void testNoMetricsEnabledByDefault() {
    // Default configuration - No Metrics enabled
    TableOperations meteredTableOps = CoreMetricsUtil.wrapWithMeterIfConfigured(conf, TableOperationsMetricType.HADOOP,
        mockTableOps);

    Assert.assertFalse("Metrics not enabled by default. TableOperations should not be an instance of Metered",
        meteredTableOps instanceof Metered);
  }

  @Test
  public void testDisableMetrics() {
    // Explicitly disable metrics
    conf.set("iceberg.dropwizard.enable-metrics-collection", "false");

    TableOperations meteredTableOps = CoreMetricsUtil.wrapWithMeterIfConfigured(conf, TableOperationsMetricType.HADOOP,
        mockTableOps);

    Assert.assertFalse("Metrics is disabled. TableOperations not should be an instance of Metered",
        meteredTableOps instanceof Metered);
  }

  @Test
  public void testEnableMetrics() {
    // Explicitly enable metrics
    conf.set("iceberg.dropwizard.enable-metrics-collection", "true");

    TableOperations meteredTableOps = CoreMetricsUtil.wrapWithMeterIfConfigured(conf, TableOperationsMetricType.HADOOP,
        mockTableOps);

    Assert.assertTrue("Metrics is enabled. TableOperations should be an instance of Metered",
        meteredTableOps instanceof Metered);
  }

  @Test
  public void testEnableMetricsStaticTableOperations() {
    conf.set("iceberg.dropwizard.enable-metrics-collection", "true");

    TableOperations meteredTableOps = CoreMetricsUtil.wrapWithMeterIfConfigured(conf, TableOperationsMetricType.STATIC,
        mockTableOps);

    Assert.assertTrue("TableOperations should be an instance of Metered", meteredTableOps instanceof Metered);
  }

}
