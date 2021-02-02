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

package org.apache.iceberg.spark;

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.actions.Spark3MigrateAction;
import org.apache.iceberg.actions.Spark3SnapshotAction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Procedure;
import org.apache.spark.sql.connector.catalog.ProcedureCatalog;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsMigrate;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.SupportsSnapshot;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseCatalog implements StagingTableCatalog, ProcedureCatalog,
    SupportsNamespaces, SupportsMigrate, SupportsSnapshot {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCatalog.class);

  @Override
  public Procedure loadProcedure(Identifier ident) throws NoSuchProcedureException {
    String[] namespace = ident.namespace();
    String name = ident.name();

    // namespace resolution is case insensitive until we have a way to configure case sensitivity in catalogs
    if (namespace.length == 1 && namespace[0].equalsIgnoreCase("system")) {
      ProcedureBuilder builder = SparkProcedures.newBuilder(name);
      if (builder != null) {
        return builder.withTableCatalog(this).build();
      }
    }

    throw new NoSuchProcedureException(ident);
  }

  @Override
  public Table migrateTable(Identifier ident, Map<String, String> properties) {
    Preconditions.checkArgument(
        provider(properties).equals("iceberg"),
        "Iceberg catalogs cannot MIGRATE to a format other than Iceberg");

    SparkSession spark = SparkSession.active();
    Spark3MigrateAction action = new Spark3MigrateAction(spark, this, ident);
    Long migratedFilesCount = action.withProperties(properties).execute();
    LOG.info("Migrated table {} and registered {} files", ident, migratedFilesCount);

    try {
      return loadTable(ident);
    } catch (NoSuchTableException e) {
      throw new org.apache.iceberg.exceptions.NoSuchTableException(
          "Migration succeeded but the table %s no longer exists in the catalog.", ident);
    }
  }

  @Override
  public Table snapshotTable(TableCatalog sourceCatalog, Identifier sourceIdent,
                             Identifier ident, Map<String, String> properties) {
    Preconditions.checkArgument(
        provider(properties).equals("iceberg"),
        "Iceberg catalogs cannot SNAPSHOT to a format other than Iceberg");

    SparkSession spark = SparkSession.active();
    Spark3SnapshotAction action = new Spark3SnapshotAction(spark, sourceCatalog, sourceIdent, this, ident);
    Long filesCount = action.withProperties(properties).execute();
    LOG.info("Created a snapshot table {} with {} files from {}.{}", ident, filesCount, sourceCatalog, sourceIdent);

    try {
      return loadTable(ident);
    } catch (NoSuchTableException e) {
      throw new org.apache.iceberg.exceptions.NoSuchTableException(
          "SNAPSHOT succeeded but the created table %s no longer exists in the catalog.", ident);
    }
  }

  private String provider(Map<String, String> properties) {
    return properties.getOrDefault(TableCatalog.PROP_PROVIDER, "iceberg").toLowerCase(Locale.ROOT);
  }
}
