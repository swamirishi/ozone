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

import static java.nio.file.Files.createDirectories;
import static org.apache.commons.io.FilenameUtils.getExtension;
import static org.apache.commons.io.file.PathUtils.deleteDirectory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class that computes delta file listings between two snapshots
 * and materializes those deltas as hard links under a designated directory.
 *
 * <p>The class provides shared utilities for implementations of
 * {@link DeltaFileComputer} that prefer to expose the computed delta as a map
 * of source filesystem paths to their corresponding hard links in the delta
 * directory. Filenames created under the configured delta directory are
 * monotonically increasing integers with the original file extension preserved.</p>
 *
 * <p>Responsibilities and behavior:
 * <ul>
 *   <li>Create and manage the delta directory supplied at construction time.</li>
 *   <li>Provide a counter-backed file naming strategy to avoid name clashes.</li>
 *   <li>Create hard links to source files via {@link #createLink(Path)} so that
 *       callers/consumers can read delta contents without moving original files.</li>
 *   <li>Expose snapshot and local-data helpers used by concrete subclasses to
 *       compute delta mappings.</li>
 *   <li>Clean up the created delta directory when {@link #close()} is invoked.</li>
 * </ul>
 * </p>
 */
public abstract class FileLinkDeltaFileComputer implements DeltaFileComputer {

  private static final Logger LOG = LoggerFactory.getLogger(FileLinkDeltaFileComputer.class);
  private final OmSnapshotManager omSnapshotManager;
  private final OMMetadataManager activeMetadataManager;
  private final Consumer<SubStatus> activityReporter;
  private Path deltaDir;
  private AtomicInteger linkFileCounter = new AtomicInteger(0);

  FileLinkDeltaFileComputer(OmSnapshotManager snapshotManager, OMMetadataManager activeMetadataManager,
      Path deltaDirPath, Consumer<SubStatus> activityReporter) throws IOException {
    this.deltaDir = deltaDirPath.toAbsolutePath();
    this.omSnapshotManager = snapshotManager;
    this.activityReporter = activityReporter;
    this.activeMetadataManager = activeMetadataManager;
    createDirectories(deltaDir);
  }

  /**
   * Compute the delta files between {@code fromSnapshot} and {@code toSnapshot}.
   *
   * <p>Concrete implementations should return an {@link Optional} containing a
   * {@link Map} whose keys are source {@link Path}s (original files containing
   * delta data) and whose values are the corresponding hard link {@link Path}s
   * created inside the configured delta directory. Implementations may create
   * the hard links using {@link #createLink(Path)} or return an equivalent
   * precomputed mapping. If no delta files are applicable, return
   * {@link Optional#empty()}.</p>
   *
   * @param fromSnapshot the baseline snapshot.
   * @param toSnapshot the target snapshot.
   * @param tablesToLookup optional set of table names used for lookups; may be empty.
   * @param tablePrefixInfo precomputed prefix info for table/bucket lookups.
   * @return an {@link Optional} containing a {@link Map} from source {@link Path}
   *         to link {@link Path}, or {@link Optional#empty()} if no delta files
   *         are applicable.
   * @throws IOException on I/O errors during computation.
   */
  abstract Optional<Map<Path, Pair<Path, SstFileInfo>>> computeDeltaFiles(SnapshotInfo fromSnapshot,
      SnapshotInfo toSnapshot, Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) throws IOException;

  public Optional<Collection<Pair<Path, SstFileInfo>>> getDeltaFiles(SnapshotInfo fromSnapshot, SnapshotInfo toSnapshot,
      Set<String> tablesToLookup) throws IOException {
    TablePrefixInfo tablePrefixInfo = activeMetadataManager.getTableBucketPrefix(fromSnapshot.getVolumeName(),
        fromSnapshot.getBucketName());
    return computeDeltaFiles(fromSnapshot, toSnapshot, tablesToLookup, tablePrefixInfo).map(Map::values);
  }

  void updateActivity(SubStatus status) {
    activityReporter.accept(status);
  }

  Path createLink(Path path) throws IOException {
    Path source = path.toAbsolutePath();
    Path link = deltaDir.resolve(linkFileCounter.incrementAndGet() +
        "." + getExtension(source.getFileName().toString()));
    try {
      Files.createLink(link, source);
    } catch (FileAlreadyExistsException ignored) {
      // This could happen if another thread tried to create the same hard link
      // and succeeded.
      LOG.debug("File for source {} already exists: at {}", source, link);
    }
    return link;
  }

  ReadableOmSnapshotLocalDataProvider getLocalDataProvider(UUID snapshotId, UUID toResolveSnapshotId)
      throws IOException {
    return omSnapshotManager.getSnapshotLocalDataManager().getOmSnapshotLocalData(snapshotId, toResolveSnapshotId);
  }

  UncheckedAutoCloseableSupplier<OmSnapshot> getSnapshot(SnapshotInfo snapshotInfo) throws IOException {
    return omSnapshotManager.getActiveSnapshot(snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
        snapshotInfo.getName());
  }

  OMMetadataManager getActiveMetadataManager() {
    return activeMetadataManager;
  }

  @Override
  public void close() throws IOException {
    if (deltaDir == null || Files.notExists(deltaDir)) {
      return;
    }
    deleteDirectory(deltaDir);
  }
}
