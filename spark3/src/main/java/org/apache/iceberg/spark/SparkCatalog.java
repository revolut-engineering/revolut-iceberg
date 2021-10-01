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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableChange.AddPartitionField;
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange;
import org.apache.spark.sql.connector.catalog.TableChange.DropIdentifierFields;
import org.apache.spark.sql.connector.catalog.TableChange.DropPartitionField;
import org.apache.spark.sql.connector.catalog.TableChange.RemoveProperty;
import org.apache.spark.sql.connector.catalog.TableChange.ReplacePartitionField;
import org.apache.spark.sql.connector.catalog.TableChange.SetIdentifierFields;
import org.apache.spark.sql.connector.catalog.TableChange.SetProperty;
import org.apache.spark.sql.connector.catalog.TableChange.SetWriteDistributionAndOrdering;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;

/**
 * A Spark TableCatalog implementation that wraps an Iceberg {@link Catalog}.
 * <p>
 * This supports the following catalog configuration options:
 * <ul>
 *   <li><code>type</code> - catalog type, "hive" or "hadoop"</li>
 *   <li><code>uri</code> - the Hive Metastore URI (Hive catalog only)</li>
 *   <li><code>warehouse</code> - the warehouse path (Hadoop catalog only)</li>
 *   <li><code>default-namespace</code> - a namespace to use as the default</li>
 *   <li><code>cache-enabled</code> - whether to enable catalog cache</li>
 * </ul>
 * <p>
 * To use a custom catalog that is not a Hive or Hadoop catalog, extend this class and override
 * {@link #buildIcebergCatalog(String, CaseInsensitiveStringMap)}.
 */
public class SparkCatalog extends BaseCatalog {
  private static final Set<String> DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER);

  private String catalogName = null;
  private Catalog icebergCatalog = null;
  private boolean cacheEnabled = true;
  private SupportsNamespaces asNamespaceCatalog = null;
  private String[] defaultNamespace = null;
  private HadoopTables tables;
  private boolean useTimestampsWithoutZone;

  /**
   * Build an Iceberg {@link Catalog} to be used by this Spark catalog adapter.
   *
   * @param name Spark's catalog name
   * @param options Spark's catalog options
   * @return an Iceberg catalog
   */
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name);
    Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    optionsMap.putAll(options);
    optionsMap.put(CatalogProperties.APP_ID, SparkSession.active().sparkContext().applicationId());
    optionsMap.put(CatalogProperties.USER, SparkSession.active().sparkContext().sparkUser());
    return CatalogUtil.buildIcebergCatalog(name, optionsMap, conf);
  }

  /**
   * Build an Iceberg {@link TableIdentifier} for the given Spark identifier.
   *
   * @param identifier Spark's identifier
   * @return an Iceberg identifier
   */
  protected TableIdentifier buildIdentifier(Identifier identifier) {
    return Spark3Util.identifierToTableIdentifier(identifier);
  }

  @Override
  public SparkTable loadTable(Identifier ident) throws NoSuchTableException {
    try {
      Table icebergTable = load(ident);
      return new SparkTable(icebergTable, !cacheEnabled);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public SparkTable createTable(Identifier ident, StructType schema,
                                Transform[] transforms,
                                Map<String, String> properties) throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    try {
      Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
      Table icebergTable = builder
          .withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
          .withLocation(properties.get("location"))
          .withProperties(Spark3Util.rebuildCreateProperties(properties))
          .create();
      return new SparkTable(icebergTable, !cacheEnabled);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageCreate(Identifier ident, StructType schema, Transform[] transforms,
                                 Map<String, String> properties) throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    try {
      Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
      Transaction transaction = builder.withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
          .withLocation(properties.get("location"))
          .withProperties(Spark3Util.rebuildCreateProperties(properties))
          .createTransaction();
      return new StagedSparkTable(transaction);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] transforms,
                                  Map<String, String> properties) throws NoSuchTableException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    try {
      Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
      Transaction transaction = builder.withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
          .withLocation(properties.get("location"))
          .withProperties(Spark3Util.rebuildCreateProperties(properties))
          .replaceTransaction();
      return new StagedSparkTable(transaction);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(Identifier ident, StructType schema, Transform[] transforms,
                                          Map<String, String> properties) {
    Schema icebergSchema = SparkSchemaUtil.convert(schema, useTimestampsWithoutZone);
    Catalog.TableBuilder builder = newBuilder(ident, icebergSchema);
    Transaction transaction = builder.withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms))
        .withLocation(properties.get("location"))
        .withProperties(Spark3Util.rebuildCreateProperties(properties))
        .createOrReplaceTransaction();
    return new StagedSparkTable(transaction);
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public SparkTable alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    SetProperty setLocation = null;
    SetProperty setSnapshotId = null;
    SetProperty pickSnapshotId = null;
    SetWriteDistributionAndOrdering setWriteDistributionAndOrdering = null;
    AddPartitionField addPartitionField = null;
    DropPartitionField dropPartitionField = null;
    ReplacePartitionField replacePartitionField = null;
    SetIdentifierFields setIdentifierFields = null;
    DropIdentifierFields dropIdentifierFields = null;
    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> schemaChanges = Lists.newArrayList();

    for (TableChange change : changes) {
      if (change instanceof SetProperty) {
        SetProperty set = (SetProperty) change;
        if (TableCatalog.PROP_LOCATION.equalsIgnoreCase(set.property())) {
          setLocation = set;
        } else if ("current-snapshot-id".equalsIgnoreCase(set.property())) {
          setSnapshotId = set;
        } else if ("cherry-pick-snapshot-id".equalsIgnoreCase(set.property())) {
          pickSnapshotId = set;
        } else if ("sort-order".equalsIgnoreCase(set.property())) {
          throw new UnsupportedOperationException("Cannot specify the 'sort-order' because it's a reserved table " +
              "property. Please use the command 'ALTER TABLE ... WRITE ORDERED BY' to specify write sort-orders.");
        } else {
          propertyChanges.add(set);
        }
      } else if (change instanceof RemoveProperty) {
        propertyChanges.add(change);
      } else if (change instanceof ColumnChange) {
        schemaChanges.add(change);
      } else if (change instanceof SetWriteDistributionAndOrdering) {
        setWriteDistributionAndOrdering = (SetWriteDistributionAndOrdering) change;
      } else if (change instanceof AddPartitionField) {
        addPartitionField = (AddPartitionField) change;
      } else if (change instanceof DropPartitionField) {
        dropPartitionField = (DropPartitionField) change;
      } else if (change instanceof ReplacePartitionField) {
        replacePartitionField = (ReplacePartitionField) change;
      } else if (change instanceof SetIdentifierFields) {
        setIdentifierFields = (SetIdentifierFields) change;
      } else if (change instanceof DropIdentifierFields) {
        dropIdentifierFields = (DropIdentifierFields) change;
      } else {
        throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
      }
    }

    try {
      Table table = load(ident);
      commitChanges(
          table, setLocation, setSnapshotId, pickSnapshotId, propertyChanges,
          schemaChanges, setWriteDistributionAndOrdering, addPartitionField,
          dropPartitionField, replacePartitionField, setIdentifierFields,
          dropIdentifierFields);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }

    return null;
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      return isPathIdentifier(ident) ?
          tables.dropTable(((PathIdentifier) ident).location()) :
          icebergCatalog.dropTable(buildIdentifier(ident));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
    try {
      checkNotPathIdentifier(from, "renameTable");
      checkNotPathIdentifier(to, "renameTable");
      icebergCatalog.renameTable(buildIdentifier(from), buildIdentifier(to));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(from);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(to);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    try {
      load(ident).refresh();
    } catch (org.apache.iceberg.exceptions.NoSuchTableException ignored) {
      // ignore if the table doesn't exist, it is not cached
    }
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    return icebergCatalog.listTables(Namespace.of(namespace)).stream()
        .map(ident -> Identifier.of(ident.namespace().levels(), ident.name()))
        .toArray(Identifier[]::new);
  }

  @Override
  public String[] defaultNamespace() {
    if (defaultNamespace != null) {
      return defaultNamespace;
    }

    return new String[0];
  }

  @Override
  public String[][] listNamespaces() {
    if (asNamespaceCatalog != null) {
      return asNamespaceCatalog.listNamespaces().stream()
          .map(Namespace::levels)
          .toArray(String[][]::new);
    }

    return new String[0][];
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.listNamespaces(Namespace.of(namespace)).stream()
            .map(Namespace::levels)
            .toArray(String[][]::new);
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.loadNamespaceMetadata(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    if (asNamespaceCatalog != null) {
      try {
        if (asNamespaceCatalog instanceof HadoopCatalog && DEFAULT_NS_KEYS.equals(metadata.keySet())) {
          // Hadoop catalog will reject metadata properties, but Spark automatically adds "owner".
          // If only the automatic properties are present, replace metadata with an empty map.
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), ImmutableMap.of());
        } else {
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), metadata);
        }
      } catch (AlreadyExistsException e) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
    } else {
      throw new UnsupportedOperationException("Namespaces are not supported by catalog: " + catalogName);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      Map<String, String> updates = Maps.newHashMap();
      Set<String> removals = Sets.newHashSet();
      for (NamespaceChange change : changes) {
        if (change instanceof NamespaceChange.SetProperty) {
          NamespaceChange.SetProperty set = (NamespaceChange.SetProperty) change;
          updates.put(set.property(), set.value());
        } else if (change instanceof NamespaceChange.RemoveProperty) {
          removals.add(((NamespaceChange.RemoveProperty) change).property());
        } else {
          throw new UnsupportedOperationException("Cannot apply unknown namespace change: " + change);
        }
      }

      try {
        if (!updates.isEmpty()) {
          asNamespaceCatalog.setProperties(Namespace.of(namespace), updates);
        }

        if (!removals.isEmpty()) {
          asNamespaceCatalog.removeProperties(Namespace.of(namespace), removals);
        }

      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    } else {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.dropNamespace(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    return false;
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    this.cacheEnabled = Boolean.parseBoolean(options.getOrDefault("cache-enabled", "true"));
    Catalog catalog = buildIcebergCatalog(name, options);

    this.catalogName = name;
    SparkSession sparkSession = SparkSession.active();
    this.useTimestampsWithoutZone = SparkUtil.useTimestampWithoutZoneInNewTables(sparkSession.conf());
    this.tables = new HadoopTables(SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name));
    this.icebergCatalog = cacheEnabled ? CachingCatalog.wrap(catalog) : catalog;
    if (catalog instanceof SupportsNamespaces) {
      this.asNamespaceCatalog = (SupportsNamespaces) catalog;
      if (options.containsKey("default-namespace")) {
        this.defaultNamespace = Splitter.on('.')
            .splitToList(options.get("default-namespace"))
            .toArray(new String[0]);
      }
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static void commitChanges(Table table, SetProperty setLocation, SetProperty setSnapshotId,
                                    SetProperty pickSnapshotId, List<TableChange> propertyChanges,
                                    List<TableChange> schemaChanges,
                                    SetWriteDistributionAndOrdering setWriteDistributionAndOrdering,
                                    AddPartitionField addPartitionField,
                                    DropPartitionField dropPartitionField,
                                    ReplacePartitionField replacePartitionField,
                                    SetIdentifierFields setIdentifierFields,
                                    DropIdentifierFields dropIdentifierFields) {
    // don't allow setting the snapshot and picking a commit at the same time because order is ambiguous and choosing
    // one order leads to different results
    Preconditions.checkArgument(setSnapshotId == null || pickSnapshotId == null,
        "Cannot set the current the current snapshot ID and cherry-pick snapshot changes");

    if (setSnapshotId != null) {
      long newSnapshotId = Long.parseLong(setSnapshotId.value());
      table.manageSnapshots().setCurrentSnapshot(newSnapshotId).commit();
    }

    // if updating the table snapshot, perform that update first in case it fails
    if (pickSnapshotId != null) {
      long newSnapshotId = Long.parseLong(pickSnapshotId.value());
      table.manageSnapshots().cherrypick(newSnapshotId).commit();
    }

    Transaction transaction = table.newTransaction();

    if (setLocation != null) {
      transaction.updateLocation()
          .setLocation(setLocation.value())
          .commit();
    }

    if (setWriteDistributionAndOrdering != null) {
      ReplaceSortOrder replaceSortOrder = transaction.replaceSortOrder();
      Spark3Util.rebuildSortOrder(replaceSortOrder, setWriteDistributionAndOrdering.ordering());
      replaceSortOrder.commit();

      String distributionModeName = setWriteDistributionAndOrdering.distributionMode();
      DistributionMode distributionMode = DistributionMode.fromName(distributionModeName);
      transaction.updateProperties()
          .set(WRITE_DISTRIBUTION_MODE, distributionMode.modeName())
          .commit();
    }

    if (addPartitionField != null) {
      transaction.updateSpec()
          .addField(addPartitionField.name(), Spark3Util.convert(addPartitionField.transform()))
          .commit();
    }

    if (dropPartitionField != null) {
      Schema schema = transaction.table().schema();
      Term term = Spark3Util.convert(dropPartitionField.transform());
      if (term instanceof NamedReference && schema.findField(((NamedReference<?>) term).name()) == null) {
        // the name is not present in the Iceberg schema, so it must be a partition field name, not a column name
        transaction.updateSpec()
            .removeField(((NamedReference<?>) term).name())
            .commit();

      } else {
        transaction.updateSpec()
            .removeField(term)
            .commit();
      }
    }

    if (replacePartitionField != null) {
      Schema schema = transaction.table().schema();
      Transform removedTransform = replacePartitionField.removedTransform();
      Term removedTerm = Spark3Util.convert(removedTransform);
      Transform addedTransform = replacePartitionField.addedTransform();
      Term addedTerm = Spark3Util.convert(addedTransform);

      if (removedTerm instanceof NamedReference && schema.findField(((NamedReference<?>) removedTerm).name()) == null) {
        // the name is not present in the Iceberg schema, so it must be a partition field name, not a column name
        transaction.updateSpec()
            .removeField(((NamedReference<?>) removedTerm).name())
            .addField(replacePartitionField.name(), addedTerm)
            .commit();

      } else {
        transaction.updateSpec()
            .removeField(Spark3Util.convert(removedTransform))
            .addField(replacePartitionField.name(), addedTerm)
            .commit();
      }
    }

    if (setIdentifierFields != null) {
      List<String> fieldNames = Arrays.stream(setIdentifierFields.fieldReferences())
          .map(Spark3Util::convert)
          .map(term -> ((NamedReference<?>) term).name())
          .collect(Collectors.toList());
      transaction.updateSchema()
          .setIdentifierFields(fieldNames)
          .commit();
    }

    if (dropIdentifierFields != null) {
      Schema schema = transaction.table().schema();
      Set<String> identifierFieldNames = Sets.newHashSet(schema.identifierFieldNames());

      Arrays.stream(dropIdentifierFields.fieldNames())
          .map(Spark3Util::convert)
          .forEach(term -> {
            String name = ((NamedReference<?>) term).name();
            Preconditions.checkArgument(schema.findField(name) != null,
                "Cannot complete drop identifier fields operation: field %s not found", name);
            Preconditions.checkArgument(identifierFieldNames.contains(name),
                "Cannot complete drop identifier fields operation: %s is not an identifier field", name);
            identifierFieldNames.remove(name);
          });

      transaction.updateSchema()
          .setIdentifierFields(identifierFieldNames)
          .commit();
    }

    if (!propertyChanges.isEmpty()) {
      Spark3Util.applyPropertyChanges(transaction.updateProperties(), propertyChanges).commit();
    }

    if (!schemaChanges.isEmpty()) {
      Spark3Util.applySchemaChanges(transaction.updateSchema(), schemaChanges).commit();
    }

    transaction.commitTransaction();
  }

  private static boolean isPathIdentifier(Identifier ident) {
    return ident instanceof PathIdentifier;
  }

  private static void checkNotPathIdentifier(Identifier identifier, String method) {
    if (identifier instanceof PathIdentifier) {
      throw new IllegalArgumentException(String.format("Cannot pass path based identifier to %s method. %s is a path.",
          method, identifier));
    }
  }

  private Table load(Identifier ident) {
    return isPathIdentifier(ident) ?
        tables.load(((PathIdentifier) ident).location()) :
        icebergCatalog.loadTable(buildIdentifier(ident));
  }

  private Catalog.TableBuilder newBuilder(Identifier ident, Schema schema) {
    return isPathIdentifier(ident) ?
        tables.buildTable(((PathIdentifier) ident).location(), schema) :
        icebergCatalog.buildTable(buildIdentifier(ident), schema);
  }
}
