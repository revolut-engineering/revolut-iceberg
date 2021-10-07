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
import org.apache.iceberg.Table;
import org.apache.iceberg.metrics.Metered;
import org.apache.iceberg.spark.source.Reader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;

public class MeteredReader extends Reader implements Metered {
  private final MetricRegistry metricRegistry;

  private static final String QUERY_PLAN_TIME = "query.plan.time";

  public MeteredReader(MetricRegistry metricRegistry, SparkSession spark, Table table, boolean caseSensitive,
                       DataSourceOptions options) {
    super(spark, table, caseSensitive, options);
    this.metricRegistry = metricRegistry;
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    try (Timer.Context ctx = metricRegistry.timer(QUERY_PLAN_TIME).time()) {
      return super.planInputPartitions();
    }
  }
}
