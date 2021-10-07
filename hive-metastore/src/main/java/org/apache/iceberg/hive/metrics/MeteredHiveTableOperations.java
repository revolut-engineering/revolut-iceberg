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

package org.apache.iceberg.hive.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.net.UnknownHostException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.Metered;
import org.apache.thrift.TException;

public class MeteredHiveTableOperations extends HiveTableOperations implements Metered {
  private final MetricRegistry metricRegistry;

  public static final String HIVE_TABLE_REFRESH_TIME = "hive.table.refresh.time";
  public static final String HIVE_TABLE_COMMIT_TIME = "hive.table.commit.time";
  public static final String HIVE_TABLE_ACQUIRE_LOCK_TIME = "hive.table.acquireLock.time";
  public static final String HIVE_TABLE_LOCK_TIME = "hive.table.lock.time";
  public static final String HIVE_TABLE_CHECK_LOCK_TIME = "hive.table.checkLock.time";
  public static final String HIVE_TABLE_UNLOCK_TIME = "hive.table.unlock.time";

  public MeteredHiveTableOperations(MetricRegistry metricRegistry, Configuration conf, ClientPool metaClients,
      FileIO fileIO, String catalogName, String database, String table) {
    super(conf, metaClients, fileIO, catalogName, database, table);
    this.metricRegistry = metricRegistry;
  }

  @Override
  public TableMetadata refresh() {
    try (Timer.Context ctx = metricRegistry.timer(MetricRegistry.name(HIVE_TABLE_REFRESH_TIME)).time()) {
      return super.refresh();
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    try (Timer.Context ctx = metricRegistry.timer(MetricRegistry.name(HIVE_TABLE_COMMIT_TIME)).time()) {
      super.commit(base, metadata);
    }
  }

  @Override
  protected long acquireLock() throws UnknownHostException, TException, InterruptedException {
    try (Timer.Context ctx = metricRegistry.timer(MetricRegistry.name(HIVE_TABLE_ACQUIRE_LOCK_TIME)).time()) {
      return super.acquireLock();
    }
  }

  @Override
  protected LockResponse lock(LockRequest lockRequest) throws TException, InterruptedException {
    try (Timer.Context ctx = metricRegistry.timer(MetricRegistry.name(HIVE_TABLE_LOCK_TIME)).time()) {
      return super.lock(lockRequest);
    }
  }

  @Override
  protected LockResponse checkLock(long checkId) throws TException, InterruptedException {
    try (Timer.Context ctx = metricRegistry.timer(MetricRegistry.name(HIVE_TABLE_CHECK_LOCK_TIME)).time()) {
      return super.checkLock(checkId);
    }
  }

  @Override
  protected void unlock(Optional<Long> lockId) {
    try (Timer.Context ctx = metricRegistry.timer(MetricRegistry.name(HIVE_TABLE_UNLOCK_TIME)).time()) {
      super.unlock(lockId);
    }
  }
}
