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

package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class TableMetadataUtil {
  private TableMetadataUtil() {
  }

  public static TableMetadata replacePaths(TableMetadata metadata,
                                           String sourcePrefix,
                                           String targetPrefix,
                                           FileIO io) {
    String newLocation = newPath(metadata.location(), sourcePrefix, targetPrefix);
    List<Snapshot> newSnapshots = updatePathInSnapshots(metadata, sourcePrefix, targetPrefix, io);
    List<MetadataLogEntry> metadataLogEntries = updatePathInMetadataLogs(metadata, sourcePrefix, targetPrefix);
    long snapshotId = metadata.currentSnapshot() == null ? -1 : metadata.currentSnapshot().snapshotId();

    return new TableMetadata(null, metadata.formatVersion(), metadata.uuid(),
        newLocation, metadata.lastSequenceNumber(), metadata.lastUpdatedMillis(), metadata.lastColumnId(),
        metadata.currentSchemaId(), metadata.schemas(), metadata.defaultSpecId(), metadata.specs(),
        metadata.lastAssignedPartitionId(), metadata.defaultSortOrderId(), metadata.sortOrders(),
        metadata.properties(), snapshotId, newSnapshots, metadata.snapshotLog(),
        metadataLogEntries);
  }

  private static List<MetadataLogEntry> updatePathInMetadataLogs(TableMetadata metadata,
                                                                 String sourcePrefix,
                                                                 String targetPrefix) {
    List<MetadataLogEntry> metadataLogEntries = Lists.newArrayListWithCapacity(metadata.previousFiles().size());
    for (MetadataLogEntry metadataLog : metadata.previousFiles()) {
      MetadataLogEntry newMetadataLog = new MetadataLogEntry(metadataLog.timestampMillis(), newPath(metadataLog.file(),
          sourcePrefix, targetPrefix));
      metadataLogEntries.add(newMetadataLog);
    }
    return metadataLogEntries;
  }

  private static List<Snapshot> updatePathInSnapshots(TableMetadata metadata,
                                                      String sourcePrefix,
                                                      String targetPrefix,
                                                      FileIO io) {
    List<Snapshot> newSnapshots = Lists.newArrayListWithCapacity(metadata.snapshots().size());
    for (Snapshot snapshot : metadata.snapshots()) {
      String newManifestListLocation = newPath(snapshot.manifestListLocation(), sourcePrefix, targetPrefix);
      Snapshot newSnapshot = new BaseSnapshot(io, snapshot.sequenceNumber(), snapshot.snapshotId(),
          snapshot.parentId(), snapshot.timestampMillis(), snapshot.operation(), snapshot.summary(),
          snapshot.schemaId(), newManifestListLocation);
      newSnapshots.add(newSnapshot);
    }
    return newSnapshots;
  }

  private static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return path.replaceFirst(sourcePrefix, targetPrefix);
  }
}
