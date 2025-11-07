/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot.diff.delta;

import static org.apache.ozone.rocksdiff.RocksDiffUtils.filterRelevantSstFiles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes snapshot deltas by comparing SST files between two snapshots and
 * materializes differing SST files as hard links in the configured delta
 * directory.
 *
 * <p>Uses an inode-aware comparison when possible and falls back to a full
 * file-list comparison on error. Delegates link creation and resource
 * cleanup to {@link FileLinkDeltaFileComputer}.</p>
 */

class FullDiffComputer extends FileLinkDeltaFileComputer {

  private static final Logger LOG = LoggerFactory.getLogger(FullDiffComputer.class);

  FullDiffComputer(OmSnapshotManager snapshotManager, OMMetadataManager activeMetadataManager, Path deltaDirPath,
      Consumer<SubStatus> activityReporter) throws IOException {
    super(snapshotManager, activeMetadataManager, deltaDirPath, activityReporter);
  }

  @Override
  Optional<Map<Path, Pair<Path, SstFileInfo>>> computeDeltaFiles(SnapshotInfo fromSnapshotInfo,
      SnapshotInfo toSnapshotInfo, Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) throws IOException {
    try (UncheckedAutoCloseableSupplier<OmSnapshot> fromSnapHandle = getSnapshot(fromSnapshotInfo);
         UncheckedAutoCloseableSupplier<OmSnapshot> toSnapHandle = getSnapshot(toSnapshotInfo)) {
      OmSnapshot fromSnapshot = fromSnapHandle.get();
      OmSnapshot toSnapshot = toSnapHandle.get();
      Path fromSnapshotPath = fromSnapshot.getMetadataManager().getStore().getDbLocation().getAbsoluteFile().toPath();
      Path toSnapshotPath = toSnapshot.getMetadataManager().getStore().getDbLocation().getAbsoluteFile().toPath();
      Map<Path, Pair<Path, SstFileInfo>> paths = new HashMap<>();
      try {
        Map<Object, SstFileInfo> fromSnapshotFiles = filterRelevantSstFiles(getSSTFileMapForSnapshot(fromSnapshot,
            tablesToLookup), tablesToLookup, tablePrefixInfo);
        Map<Object, SstFileInfo> toSnapshotFiles = filterRelevantSstFiles(getSSTFileMapForSnapshot(toSnapshot,
            tablesToLookup), tablesToLookup, tablePrefixInfo);
        for (Map.Entry<Object, SstFileInfo> entry : fromSnapshotFiles.entrySet()) {
          if (!toSnapshotFiles.containsKey(entry.getKey())) {
            Path source = entry.getValue().getFilePath(fromSnapshotPath);
            paths.put(source, Pair.of(createLink(source), entry.getValue()));
          }
        }
        for (Map.Entry<Object, SstFileInfo> entry : toSnapshotFiles.entrySet()) {
          if (!fromSnapshotFiles.containsKey(entry.getKey())) {
            Path source = entry.getValue().getFilePath(fromSnapshotPath);
            paths.put(source, Pair.of(createLink(source), entry.getValue()));
          }
        }
      } catch (IOException e) {
        // In case of exception during inode read use all files
        LOG.error("Exception occurred while populating delta files for snapDiff", e);
        LOG.warn("Falling back to full file list comparison, inode-based optimization skipped.");
        paths.clear();
        Set<SstFileInfo> fromSnapshotFiles = filterRelevantSstFiles(getSSTFileListForSnapshot(fromSnapshot,
                tablesToLookup), tablesToLookup, tablePrefixInfo);
        Set<SstFileInfo> toSnapshotFiles = filterRelevantSstFiles(getSSTFileListForSnapshot(toSnapshot,
            tablesToLookup), tablesToLookup, tablePrefixInfo);
        for (SstFileInfo sstFileInfo : fromSnapshotFiles) {
          Path source = sstFileInfo.getFilePath(fromSnapshotPath);
          paths.put(source, Pair.of(createLink(source), sstFileInfo));
        }
        for (SstFileInfo sstFileInfo : toSnapshotFiles) {
          Path source = sstFileInfo.getFilePath(toSnapshotPath);
          paths.put(source, Pair.of(createLink(source), sstFileInfo));
        }
      }
      return Optional.of(paths);
    }
  }

  static Map<Object, SstFileInfo> getSSTFileMapForSnapshot(OmSnapshot snapshot,
      Set<String> tablesToLookUp) throws IOException {
    return RdbUtil.getSSTFilesWithInodesForComparison(((RDBStore)snapshot
            .getMetadataManager().getStore()).getDb().getManagedRocksDb(), tablesToLookUp);
  }

  static Set<SstFileInfo> getSSTFileListForSnapshot(OmSnapshot snapshot, Set<String> tablesToLookUp) {
    return RdbUtil.getSSTFilesForComparison(
        ((RDBStore)snapshot.getMetadataManager().getStore()).getDb().getManagedRocksDb(), tablesToLookUp);
  }
}
