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
import com.codahale.metrics.Timer;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import org.apache.iceberg.metrics.CoreMetricsUtil;
import org.apache.iceberg.metrics.CoreMetricsUtil.TableOperationsMetricType;
import org.apache.iceberg.metrics.MeteredTableOperations;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.metrics.sink.Sink;
import org.apache.spark.metrics.source.Source;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConverters;

import static org.junit.Assert.fail;

public class TestIcebergMetricSource {
  private static MetricsSystem metricsSystem;
  private static IcebergMetricSource icebergMetricSource;
  private static MockSink mockSink;
  private static final String HADOOP_METRIC_PREFIX = TableOperationsMetricType.HADOOP.prefix();

  @BeforeClass
  public static void startBefore() {
    SparkConf conf = new SparkConf(false)
        .set("spark.metrics.conf.*.source.iceberg.class", "org.apache.iceberg.spark.metrics.IcebergMetricSource")
        .set("spark.metrics.conf.*.sink.mocksink.class",
            "org.apache.iceberg.spark.metrics.TestIcebergMetricSource$MockSink");
    SecurityManager securityManager = new SecurityManager(conf, Option.empty());

    Assert.assertNull("MockSink should be null. MetricsSystem is not initialized yet.", MockSink.instance);
    metricsSystem = MetricsSystem.createMetricsSystem("default", conf, securityManager);
    metricsSystem.start(true);
    Assert.assertNotNull("MockSink should not be null. MetricsSystem is initialized.", MockSink.instance);

    List<Source> sources = JavaConverters
        .seqAsJavaListConverter(metricsSystem.getSourcesByName("iceberg"))
        .asJava();
    Assert.assertEquals("Exactly one source should be there for iceberg", 1, sources.size());
    Source source = sources.get(0);
    Assert.assertTrue("iceberg metric source should be an instance of IcebergMetricSource",
        source instanceof IcebergMetricSource);

    icebergMetricSource = (IcebergMetricSource) sources.get(0);
    mockSink = MockSink.instance;
  }

  @AfterClass
  public static void stopMetricSystem() {
    metricsSystem.stop();
    metricsSystem = null;
    mockSink = null;
    icebergMetricSource = null;
  }

  @Test
  public void testIfMetersArePropagated() {

    // Create a new timer - but expect that this timer is pre-created via
    // {@link MeteredTableOperations#preRegisterMetrics()}.
    Timer internalTimer = CoreMetricsUtil.timer(HADOOP_METRIC_PREFIX, MeteredTableOperations.TABLE_COMMIT_TIME);

    // The new timer should be available in Spark's metrics source.
    Timer sourceTimer = findTimer(icebergMetricSource.metricRegistry(),
        MetricRegistry.name(HADOOP_METRIC_PREFIX, MeteredTableOperations.TABLE_COMMIT_TIME));

    Assert.assertNotNull("Source Timer should not be null", sourceTimer);
    Assert.assertEquals("Internal and source timers should be the same", internalTimer, sourceTimer);

    // The new timer should be available in Spark's metrics sink also.
    Timer sinkTimer = findTimer(mockSink.metricRegistry,
        MetricRegistry.name(HADOOP_METRIC_PREFIX, MeteredTableOperations.TABLE_COMMIT_TIME));

    Assert.assertNotNull("Sink Timer should not be null", sinkTimer);
    Assert.assertEquals("Source and sink timers should be the same", sourceTimer, sinkTimer);
  }

  @Test
  public void testChildRegistriesAreTrackedByParent() {
    String newKey = "some.metric.key";

    // Create a new timer
    CoreMetricsUtil.timer(HADOOP_METRIC_PREFIX, newKey);

    // The new timer should be available in Spark's metrics source.
    try {
      findTimer(icebergMetricSource.metricRegistry(), MetricRegistry.name(HADOOP_METRIC_PREFIX, newKey));
    } catch (NoSuchElementException e) {
      fail("Child registries must be tracked by parent");
    }

    // The new timer should be available in Spark's metrics sink (obviously).
    try {
      findTimer(mockSink.metricRegistry, MetricRegistry.name(HADOOP_METRIC_PREFIX, newKey));
    } catch (NoSuchElementException e) {
      fail("Child registries must be tracked by parent");
    }
  }

  private Timer findTimer(MetricRegistry metricRegistry, String keyEndsWith) {
    return metricRegistry
        .getTimers()
        .entrySet()
        .stream()
        .filter(e -> e.getKey().endsWith(keyEndsWith))
        .findFirst()
        .orElseThrow(NoSuchElementException::new)
        .getValue();
  }

  public static class MockSink implements Sink {

    private final Properties properties;
    private final MetricRegistry metricRegistry;
    private final SecurityManager securityManager;
    protected static MockSink instance = null;

    public MockSink(Properties properties, MetricRegistry metricRegistry, SecurityManager securityManager) {
      this.properties = properties;
      this.metricRegistry = metricRegistry;
      this.securityManager = securityManager;
      instance = this;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
      instance = null;
    }

    @Override
    public void report() {
    }
  }
}
