/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;

/**
 * Thread-safe custom unbounded LRU cache to manage open snapshot DB instances.
 */
public class SnapshotCache implements ReferenceCountedCallback, AutoCloseable {

  static final Logger LOG = LoggerFactory.getLogger(SnapshotCache.class);

  // Snapshot cache internal hash map.
  // Key:   DB snapshot table key
  // Value: OmSnapshot instance, each holds a DB instance handle inside
  // TODO: [SNAPSHOT] Consider wrapping SoftReference<> around IOmMetadataReader
  private final ConcurrentHashMap<String, ReferenceCounted<OmSnapshot>> dbMap;

  private final CacheLoader<String, OmSnapshot> cacheLoader;
  // Soft-limit of the total number of snapshot DB instances allowed to be
  // opened on the OM.
  private final int cacheSizeLimit;
  private final Set<String> pendingEvictionQueue;
  private final Scheduler scheduler;
  private static final String SNAPSHOT_CACHE_CLEANUP_SERVICE =
      "SnapshotCacheCleanupService";

  public SnapshotCache(
      CacheLoader<String, OmSnapshot> cacheLoader,
      int cacheSizeLimit, long cleanupInterval) {
    this.dbMap = new ConcurrentHashMap<>();
    this.cacheLoader = cacheLoader;
    this.cacheSizeLimit = cacheSizeLimit;
    this.pendingEvictionQueue = ConcurrentHashMap.newKeySet();
    if (cleanupInterval > 0) {
      this.scheduler = new Scheduler(SNAPSHOT_CACHE_CLEANUP_SERVICE,
          true, 1);
      this.scheduler.scheduleWithFixedDelay(this::cleanup, cleanupInterval,
          cleanupInterval, TimeUnit.MILLISECONDS);
    } else {
      this.scheduler = null;
    }
  }

  @VisibleForTesting
  ConcurrentHashMap<String, ReferenceCounted<OmSnapshot>> getDbMap() {
    return dbMap;
  }

  /**
   * @return number of DB instances currently held in cache.
   */
  public int size() {
    return dbMap.size();
  }

  /**
   * Immediately invalidate an entry.
   * @param key DB snapshot table key
   */
  public void invalidate(String key) {
    dbMap.compute(key, (k, v) -> {
      if (v == null) {
        LOG.warn("Key: '{}' does not exist in cache.", k);
      } else {
        try {
          v.get().close();
        } catch (IOException e) {
          throw new IllegalStateException("Failed to close snapshot: " + key, e);
        }
      }
      return null;
    });
  }

  /**
   * Immediately invalidate all entries and close their DB instances in cache.
   */
  public void invalidateAll() {
    for (String key : dbMap.keySet()) {
      invalidate(key);
    }
  }

  @Override
  public void close() {
    invalidateAll();
    if (this.scheduler != null) {
      this.scheduler.close();
    }
  }

  /**
   * State the reason the current thread is getting the OmSnapshot instance.
   * Unused for now.
   */
  public enum Reason {
    FS_API_READ,
    SNAPDIFF_READ,
    DEEP_CLEAN_WRITE,
    GARBAGE_COLLECTION_WRITE
  }

  /**
   * Get or load OmSnapshot. Shall be close()d after use.
   * TODO: [SNAPSHOT] Can add reason enum to param list later.
   * @param key snapshot table key
   * @return an OmSnapshot instance, or null on error
   */
  public ReferenceCounted<OmSnapshot> get(String key)
      throws IOException {
    // Warn if actual cache size exceeds the soft limit already.
    if (size() > cacheSizeLimit) {
      LOG.warn("Snapshot cache size ({}) exceeds configured soft-limit ({}).",
          size(), cacheSizeLimit);
    }
    // Atomic operation to initialize the OmSnapshot instance (once) if the key
    // does not exist, and increment the reference count on the instance.
    ReferenceCounted<OmSnapshot> rcOmSnapshot =
        dbMap.compute(key, (k, v) -> {
          if (v == null) {
            LOG.info("Loading snapshot. Table key: {}", k);
            try {
              v = new ReferenceCounted<>(cacheLoader.load(k), false, this);
            } catch (OMException omEx) {
              // Return null if the snapshot is no longer active
              if (!omEx.getResult().equals(FILE_NOT_FOUND)) {
                throw new IllegalStateException(omEx);
              }
            } catch (IOException ioEx) {
              // Failed to load snapshot DB
              throw new IllegalStateException(ioEx);
            } catch (Exception ex) {
              // Unexpected and unknown exception thrown from CacheLoader#load
              throw new IllegalStateException(ex);
            }
          }
          if (v != null) {
            // When RC OmSnapshot is successfully loaded
            v.incrementRefCount();
          }
          return v;
        });

    if (rcOmSnapshot == null) {
      // The only exception that would fall through the loader logic above
      // is OMException with FILE_NOT_FOUND.
      throw new OMException("Snapshot table key '" + key + "' not found, "
          + "or the snapshot is no longer active",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }
    return rcOmSnapshot;
  }

  /**
   * Release the reference count on the OmSnapshot instance.
   * @param key snapshot table key
   */
  public void release(String key) {
    dbMap.compute(key, (k, v) -> {
      if (v == null) {
        throw new IllegalArgumentException("Key '" + key + "' does not exist in cache.");
      } else {
        v.decrementRefCount();
      }
      return v;
    });
  }

  /**
   * Alternatively, can release with OmSnapshot instance directly.
   * @param omSnapshot OmSnapshot
   */
  public void release(OmSnapshot omSnapshot) {
    final String snapshotTableKey = omSnapshot.getSnapshotTableKey();
    release(snapshotTableKey);
  }

  /**
   * If cache size exceeds soft limit, attempt to clean up and close the
   instances that has zero reference count.
   */
  @VisibleForTesting
  void cleanup() {
    if (dbMap.size() > cacheSizeLimit) {
      for (String key : pendingEvictionQueue) {
        dbMap.compute(key, (k, v) -> {
          pendingEvictionQueue.remove(k);
          if (v == null) {
            throw new IllegalStateException("SnapshotId '" + k + "' does not exist in cache. The RocksDB " +
                "instance of the Snapshot may not be closed properly.");
          }

          if (v.getTotalRefCount() > 0) {
            LOG.debug("SnapshotId {} is still being referenced ({}), skipping its clean up.", k, v.getTotalRefCount());
            return v;
          } else {
            LOG.debug("Closing SnapshotId {}. It is not being referenced anymore.", k);
            // Close the instance, which also closes its DB handle.
            try {
              v.get().close();
            } catch (IOException ex) {
              throw new IllegalStateException("Error while closing snapshot DB.", ex);
            }
            return null;
          }
        });
      }
    }
  }

  /**
   * Callback method used to enqueue or dequeue ReferenceCounted from
   * pendingEvictionList.
   * @param referenceCounted ReferenceCounted object
   */
  @Override
  public void callback(ReferenceCounted referenceCounted) {
    if (referenceCounted.getTotalRefCount() == 0L) {
      // Reference count reaches zero, add to pendingEvictionList
      pendingEvictionQueue.add(((OmSnapshot) referenceCounted.get()).getSnapshotTableKey());
    }
  }
}
