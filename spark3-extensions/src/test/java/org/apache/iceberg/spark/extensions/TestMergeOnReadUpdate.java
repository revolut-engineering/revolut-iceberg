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


package org.apache.iceberg.spark.extensions;

import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;

public class TestMergeOnReadUpdate extends TestUpdate {

  public TestMergeOnReadUpdate(String catalogName, String implementation, Map<String, String> config,
                               String fileFormat, boolean vectorized, String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  // TODO: enable ORC tests once we support ORC delete files

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}," +
      " format = {3}, vectorized = {4}, distributionMode = {5}")
  public static Object[][] parameters() {
    return new Object[][]{
        {"testhive", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default"
            ),
            "parquet",
            false,
            WRITE_DISTRIBUTION_MODE_NONE
        },
        {"testhadoop", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hadoop"
            ),
            "parquet",
            true,
            WRITE_DISTRIBUTION_MODE_HASH
        },
        {"spark_catalog", SparkSessionCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default",
                "clients", "1",
                "parquet-enabled", "false",
                "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
            "avro",
            false,
            WRITE_DISTRIBUTION_MODE_RANGE
        }
    };
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.FORMAT_VERSION, "2",
        TableProperties.UPDATE_MODE, "merge-on-read"
    );
  }
}
