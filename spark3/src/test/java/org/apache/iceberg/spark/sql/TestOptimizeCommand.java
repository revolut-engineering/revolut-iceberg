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

package org.apache.iceberg.spark.sql;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.actions.Spark3SortStrategy;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Assert;
import org.junit.Test;

public class TestOptimizeCommand extends SparkCatalogTestBase {

  public TestOptimizeCommand(String catalogName, String implementation,
      Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testOptimizeSortCommand() {
    createTable();
    sql("ALTER TABLE %s WRITE ORDERED BY c3", tableName);
    List<Object[]> rows = sql("OPTIMIZE %s SORT %s", tableName, defaultSortOptions());
    ImmutableList<Object[]> expected = ImmutableList.of(row("ALL", 20, 4));
    assertEquals("Should have correct results", expected, rows);
  }

  @Test
  public void testOptimizeSortCommandCustomOrder() {
    createTable();
    List<Object[]> rows = sql("OPTIMIZE %s SORT BY c2 %s", tableName, defaultSortOptions());
    ImmutableList<Object[]> expected = ImmutableList.of(row("ALL", 20, 4));
    assertEquals("Should have correct results", expected, rows);
  }

  @Test
  public void testOptimizeSortCommandPartitioned() {
    createPartitionedTable();
    sql("ALTER TABLE %s WRITE ORDERED BY c3", tableName);
    List<Object[]> rows = sql("OPTIMIZE %s SORT %s", tableName, defaultSortOptions());
    rows.sort(Comparator.comparing(l -> (String) l[0]));
    ImmutableList<Object[]> expected = ImmutableList.of(
        row("ALL", 50, 10),
        row("PartitionData{c1=0}", 10, 2),
        row("PartitionData{c1=1}", 10, 2),
        row("PartitionData{c1=2}", 10, 2),
        row("PartitionData{c1=3}", 10, 2),
        row("PartitionData{c1=4}", 10, 2)
    );
    assertEquals("Should have correct results", expected, rows);
  }

  @Test
  public void testOptimizeSortCommandFilter() {
    createPartitionedTable();
    sql("ALTER TABLE %s WRITE ORDERED BY c3", tableName);
    List<Object[]> rows = sql("OPTIMIZE %s WHERE c1=0 SORT %s ", tableName, defaultSortOptions());
    rows.sort(Comparator.comparing(l -> (String) l[0]));
    ImmutableList<Object[]> expected = ImmutableList.of(
        row("ALL", 10, 2),
        row("PartitionData{c1=0}", 10, 2)
    );
    assertEquals("Should have correct results", expected, rows);
  }

  @Test
  public void testOptimizeBinpack() {
    createTable();
    sql("ALTER TABLE %s WRITE ORDERED BY c3", tableName);
    List<Object[]> rows = sql("OPTIMIZE %s %s", tableName, defaultSortOptions());
    ImmutableList<Object[]> expected = ImmutableList.of(row("ALL", 20, 5));
    assertEquals("Should have correct results", expected, rows);
  }

  @Test
  public void testOptimizeBinpackPartitioned() {
    createPartitionedTable();
    sql("ALTER TABLE %s WRITE ORDERED BY c3", tableName);
    List<Object[]> rows = sql("OPTIMIZE %s WHERE c1=3", tableName, defaultSortOptions());
    rows.sort(Comparator.comparing(l -> (String) l[0]));
    ImmutableList<Object[]> expected = ImmutableList.of(
        row("ALL", 10, 1),
        row("PartitionData{c1=3}", 10, 1)
    );
    assertEquals("Should have correct results", expected, rows);
  }

  @Test
  public void testOptimizeUnpartitionedWithMultipleShuffleTasksPerFile() {
    createTable();

    Table table = validationCatalog.loadTable(tableIdent);
    shouldHaveLastCommitUnsorted(table, "c2");

    List<Object[]> rows = sql(
        "OPTIMIZE TABLE %s " +
        "ORDER BY c2, c3 " +
        "OPTIONS ('%s'='%d', '%s'='%d')",
        tableName, Spark3SortStrategy.SHUFFLE_TASKS_PER_FILE, 3,
        RewriteDataFiles.TARGET_FILE_SIZE_BYTES, averageFileSize(table) * 5);

    ImmutableList<Object[]> expected = ImmutableList.of(row("ALL", 20, 4));
    assertEquals("Should have correct results", expected, rows);

    table.refresh();

    shouldHaveLastCommitSorted(table, "c2");
    shouldHaveLastCommitSorted(table, "c3");
  }

  @Test
  public void testOptimizePartitionedWithMultipleShuffleTasksPerFile() {
    createBucketedTable();

    Table table = validationCatalog.loadTable(tableIdent);

    List<Object[]> rows = sql(
        "OPTIMIZE TABLE %s " +
        "ORDER BY bucket(4, c3), c2, c1 " +
        "OPTIONS ('%s'='%d', '%s'='%d')",
        tableName, Spark3SortStrategy.SHUFFLE_TASKS_PER_FILE, 2,
        RewriteDataFiles.TARGET_FILE_SIZE_BYTES, averageFileSize(table) * 5);

    rows.sort(Comparator.comparing(l -> (String) l[0]));
    ImmutableList<Object[]> expected = ImmutableList.of(
        row("ALL", 40, 8),
        row("PartitionData{c3_bucket=0}", 10, 2),
        row("PartitionData{c3_bucket=1}", 10, 2),
        row("PartitionData{c3_bucket=2}", 10, 2),
        row("PartitionData{c3_bucket=3}", 10, 2)
    );
    assertEquals("Should have correct results", expected, rows);
  }

  @Test
  public void testInvalidOptions() {
    createTable();

    AssertHelpers.assertThrows("Should throw a sensible exception with bad filter",
        AnalysisException.class, "Could not translate",
        () -> sql("OPTIMIZE %s WHERE c3 = 2 %s", tableName, defaultSortOptions()));

    AssertHelpers.assertThrows("Should throw a sensible error with a bad SortOrder",
        UnsupportedOperationException.class, "Transform is not supported",
        () -> sql("OPTIMIZE %s SORT BY cos(c3) %s", tableName, defaultSortOptions()));
  }

  private String defaultSortOptions() {
    return String.format("OPTIONS ('%s'='%s', '%s'='%s', '%s'='%s')",
        BinPackStrategy.MIN_INPUT_FILES, 1,
        BinPackStrategy.MIN_FILE_SIZE_BYTES, averageFileSize * 2,
        RewriteDataFiles.TARGET_FILE_SIZE_BYTES, averageFileSize * 5);
  }

  private long averageFileSize = 1698;

  private void createTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("CREATE TABLE %s (c1 int, c2 string, c3 string) using iceberg", tableName);
    List<ThreeColumnRecord> records1 = Lists.newArrayList();

    List<Integer> data = IntStream.range(0, 2000).boxed().collect(Collectors.toList());
    Collections.shuffle(data, new Random(42));
    data.forEach(i -> records1.add(new ThreeColumnRecord(i, "foo" + i, "bar" + i)));
    Dataset<Row> df = spark.createDataFrame(records1, ThreeColumnRecord.class).repartition(20);
    try {
      df.writeTo(tableName).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private void createPartitionedTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("CREATE TABLE %s (c1 int, c2 string, c3 string) using iceberg PARTITIONED BY (c1)", tableName);
    List<ThreeColumnRecord> records1 = Lists.newArrayList();

    List<Integer> data = IntStream.range(0, 2000).boxed().collect(Collectors.toList());
    Collections.shuffle(data, new Random(42));
    data.forEach(i -> records1.add(new ThreeColumnRecord(i % 5, "foo" + i, "bar" + i)));
    Dataset<Row> df = spark.createDataFrame(records1, ThreeColumnRecord.class).repartition(10);
    try {
      df.writeTo(tableName).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private void createBucketedTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("CREATE TABLE %s (c1 int, c2 string, c3 string) using iceberg PARTITIONED BY (bucket(4, c3))", tableName);
    List<ThreeColumnRecord> records1 = Lists.newArrayList();

    List<Integer> data = IntStream.range(0, 2000).boxed().collect(Collectors.toList());
    Collections.shuffle(data, new Random(42));
    data.forEach(i -> records1.add(new ThreeColumnRecord(i % 5, "foo" + i, "bar" + i)));
    Dataset<Row> df = spark.createDataFrame(records1, ThreeColumnRecord.class).repartition(10);
    try {
      df.writeTo(tableName).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  protected int averageFileSize(Table table) {
    return (int) Streams.stream(table.currentSnapshot().addedFiles().iterator())
        .mapToLong(DataFile::fileSizeInBytes)
        .average()
        .getAsDouble();
  }

  protected <T> void shouldHaveLastCommitSorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>>
        overlappingFiles = getOverlappingFiles(table, column);

    Assert.assertEquals("Found overlapping files", Collections.emptyList(), overlappingFiles);
  }

  protected <T> void shouldHaveLastCommitUnsorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>>
        overlappingFiles = getOverlappingFiles(table, column);

    Assert.assertNotEquals("Found overlapping files", Collections.emptyList(), overlappingFiles);
  }

  private <T> List<Pair<Pair<T, T>, Pair<T, T>>> getOverlappingFiles(Table table, String column) {
    Types.NestedField field = table.schema().caseInsensitiveFindField(column);
    int columnId = field.fieldId();
    Class<T> javaClass = (Class<T>) field.type().typeId().javaClass();
    List<Pair<T, T>> columnBounds =
        Streams.stream(table.currentSnapshot().addedFiles())
            .map(file -> Pair.of(
                javaClass.cast(Conversions.fromByteBuffer(field.type(), file.lowerBounds().get(columnId))),
                javaClass.cast(Conversions.fromByteBuffer(field.type(), file.upperBounds().get(columnId)))))
            .collect(Collectors.toList());

    Comparator<T> comparator = Comparators.forType(field.type().asPrimitiveType());

    List<Pair<Pair<T, T>, Pair<T, T>>> overlappingFiles = columnBounds.stream()
        .flatMap(left -> columnBounds.stream().map(right -> Pair.of(left, right)))
        .filter(filePair -> {
          Pair<T, T> left = filePair.first();
          T leftLower = left.first();
          T leftUpper = left.second();
          Pair<T, T> right = filePair.second();
          T rightLower = right.first();
          T rightUpper = right.second();
          boolean boundsOverlap =
              (comparator.compare(leftUpper, rightLower) > 0 && comparator.compare(leftUpper, rightUpper) < 0) ||
                  (comparator.compare(leftLower, rightLower) > 0 && comparator.compare(leftLower, rightUpper) < 0);

          return (left != right) && boundsOverlap;
        })
        .collect(Collectors.toList());
    return overlappingFiles;
  }
}
