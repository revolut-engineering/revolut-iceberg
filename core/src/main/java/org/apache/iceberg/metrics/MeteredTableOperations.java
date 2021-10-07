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
import com.codahale.metrics.Timer.Context;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.metrics.CoreMetricsUtil.TableOperationsMetricType;

public class MeteredTableOperations implements TableOperations, Metered {

  private final MetricRegistry metricRegistry;
  private final TableOperations delegate;
  private final TableOperationsMetricType type;

  public static final String TABLE_REFRESH_TIME = "table.refresh.time";
  public static final String TABLE_COMMIT_TIME = "table.commit.time";

  public MeteredTableOperations(MetricRegistry metricRegistry, TableOperationsMetricType type,
      TableOperations delegate) {
    this.type = type;
    this.delegate = delegate;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public TableMetadata current() {
    return delegate.current();
  }

  @Override
  public TableMetadata refresh() {
    try (Context ctx = metricRegistry.timer(MetricRegistry.name(type.prefix(), TABLE_REFRESH_TIME)).time()) {
      return delegate.refresh();
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    try (Context ctx = metricRegistry.timer(MetricRegistry.name(type.prefix(), TABLE_COMMIT_TIME)).time()) {
      delegate.commit(base, metadata);
    }
  }

  @Override
  public FileIO io() {
    return delegate.io();
  }

  @Override
  public EncryptionManager encryption() {
    return delegate.encryption();
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return delegate.metadataFileLocation(fileName);
  }

  @Override
  public LocationProvider locationProvider() {
    return delegate.locationProvider();
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return delegate.temp(uncommittedMetadata);
  }

  @Override
  public long newSnapshotId() {
    return delegate.newSnapshotId();
  }
}
