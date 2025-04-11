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

package org.apache.hadoop.hdds.utils.db.managed;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Managed {@link RocksDB}.
 */
public class ManagedRocksDB extends ManagedObject<RocksDB> {
  public static final Class<RocksDB> ORIGINAL_CLASS = RocksDB.class;
  public static final int NOT_FOUND = RocksDB.NOT_FOUND;

  static final Logger LOG = LoggerFactory.getLogger(ManagedRocksDB.class);

  private Set<ManagedSnapshot> snapshots;
  private ReadWriteLock dbHandleLock;

  ManagedRocksDB(RocksDB original) {
    super(original);
    snapshots = Sets.newConcurrentHashSet();
    dbHandleLock = new ReentrantReadWriteLock();
  }

  public static ManagedRocksDB openReadOnly(
      final ManagedDBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB openReadOnly(
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB open(
      final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.open(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB open(
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.open(path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  /**
   * Delete liveMetaDataFile from rocks db using RocksDB#deleteFile Api.
   * This function makes the RocksDB#deleteFile Api synchronized by waiting
   * for the deletes to happen.
   * @param fileToBeDeleted File to be deleted.
   * @throws RocksDBException In the underlying db throws an exception.
   * @throws IOException In the case file is not deleted.
   */
  public void deleteFile(LiveFileMetaData fileToBeDeleted)
      throws RocksDBException, IOException {
    String sstFileName = fileToBeDeleted.fileName();
    this.get().deleteFile(sstFileName);
    File file = new File(fileToBeDeleted.path(), fileToBeDeleted.fileName());
    ManagedRocksObjectUtils.waitForFileDelete(file, Duration.ofSeconds(60));
  }


  /**
   * Get a point in time view of the rocksdb based on the Sequence number of rocksdb.
   * @return Handle to the Snapshot taken which can be used to view at a consistent view of the db.
   * @throws RocksDBException
   */
  public ManagedSnapshot takeSnapshot() throws RocksDBException {
    dbHandleLock.readLock().lock();
    try {
      if (isClosed()) {
        return null;
      }
      Consumer<ManagedSnapshot> snapshotCloseHandler = managedSnapshot -> {
        dbHandleLock.readLock().unlock();
        try {
          if (!isClosed() && !managedSnapshot.isClosed()) {
            this.get().releaseSnapshot(managedSnapshot.get());
            this.snapshots.remove(managedSnapshot);
          }
        } finally {
          dbHandleLock.readLock().unlock();
        }
      };
      ManagedSnapshot snapshot = ManagedSnapshot.newManagedSnapshots(this.get().getSnapshot(), snapshotCloseHandler);
      this.snapshots.add(snapshot);
      return snapshot;
    } finally {
      dbHandleLock.readLock().unlock();
    }
  }

  public static Map<String, LiveFileMetaData> getLiveMetadataForSSTFiles(RocksDB db) {
    return db.getLiveFilesMetaData().stream().collect(
            Collectors.toMap(liveFileMetaData -> FilenameUtils.getBaseName(liveFileMetaData.fileName()),
                liveFileMetaData -> liveFileMetaData));
  }

  public Map<String, LiveFileMetaData> getLiveMetadataForSSTFiles() {
    return getLiveMetadataForSSTFiles(this.get());
  }

  @Override
  public synchronized void close() {
    dbHandleLock.writeLock().lock();
    try {
      for (ManagedSnapshot snapshot : snapshots) {
        snapshot.close();
      }
      super.close();
    } finally {
      dbHandleLock.writeLock().unlock();
    }


  }
}
