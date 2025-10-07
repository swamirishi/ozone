/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;

import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMDirectoriesPurgeResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeDirectoriesRequest;

import java.util.List;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.validatePreviousSnapshotId;

/**
 * Handles purging of keys from OM DB.
 */
public class OMDirectoriesPurgeRequestWithFSO extends OMKeyRequest {

  public OMDirectoriesPurgeRequestWithFSO(OMRequest omRequest) {
    super(omRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    PurgeDirectoriesRequest purgeDirsRequest =
        getOmRequest().getPurgeDirectoriesRequest();
    String fromSnapshot = purgeDirsRequest.hasSnapshotTableKey() ?
        purgeDirsRequest.getSnapshotTableKey() : null;

    List<OzoneManagerProtocolProtos.PurgePathRequest> purgeRequests =
        purgeDirsRequest.getDeletedPathList();
    Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap = new HashMap<>();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    DeletingServiceMetrics deletingServiceMetrics = ozoneManager.getDeletionMetrics();
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    final SnapshotInfo fromSnapshotInfo;
    try {
      fromSnapshotInfo = fromSnapshot != null ? SnapshotUtils.getSnapshotInfo(ozoneManager,
          fromSnapshot) : null;
      // Checking if this request is an old request or new one.
      if (purgeDirsRequest.hasExpectedPreviousSnapshotID()) {
        // Validating previous snapshot since while purging deletes, a snapshot create request could make this purge
        // directory request invalid on AOS since the deletedDirectory would be in the newly created snapshot. Adding
        // subdirectories could lead to not being able to reclaim sub-files and subdirectories since the
        // file/directory would be present in the newly created snapshot.
        // Validating previous snapshot can ensure the chain hasn't changed.
        UUID expectedPreviousSnapshotId = purgeDirsRequest.getExpectedPreviousSnapshotID().hasUuid()
            ? fromProtobuf(purgeDirsRequest.getExpectedPreviousSnapshotID().getUuid()) : null;
        validatePreviousSnapshotId(fromSnapshotInfo, omMetadataManager.getSnapshotChainManager(),
            expectedPreviousSnapshotId);
      }
    } catch (IOException e) {
      LOG.error("Error occurred while performing OMDirectoriesPurge. ", e);
      return new OMDirectoriesPurgeResponseWithFSO(createErrorOMResponse(omResponse, e));
    }
    List<String[]> bucketLockKeys = getBucketLockKeySet(purgeDirsRequest);
    boolean lockAcquired = omMetadataManager.getLock().acquireWriteLocks(BUCKET_LOCK, bucketLockKeys).isLockAcquired();
    if (!lockAcquired && !purgeDirsRequest.getBucketNameInfosList().isEmpty()) {
      OMException oe = new OMException("Unable to acquire write locks on buckets while performing DirectoryPurge",
          OMException.ResultCodes.KEY_DELETION_ERROR);
      LOG.error("Error occurred while performing OMDirectoriesPurge. ", oe);
      return new OMDirectoriesPurgeResponseWithFSO(createErrorOMResponse(omResponse, oe));
    }
    try {
      int numSubDirMoved = 0, numSubFilesMoved = 0, numDirsDeleted = 0;
      for (OzoneManagerProtocolProtos.PurgePathRequest path : purgeRequests) {
        for (OzoneManagerProtocolProtos.KeyInfo key :
            path.getMarkDeletedSubDirsList()) {
          ProcessedKeyInfo processed = processDeleteKey(key, path, omMetadataManager);

          omMetrics.decNumKeys();
          numSubDirMoved++;
          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
              processed.volumeName, processed.bucketName);
          // bucketInfo can be null in case of delete volume or bucket
          // or key does not belong to bucket as bucket is recreated
          if (null != omBucketInfo
              && omBucketInfo.getObjectID() == path.getBucketId()) {
            omBucketInfo.incrUsedNamespace(-1L);
            String ozoneDbKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
                path.getBucketId(), processed.keyInfo.getParentObjectID(),
                processed.keyInfo.getFileName());
            omMetadataManager.getDirectoryTable().addCacheEntry(new CacheKey<>(ozoneDbKey),
                CacheValue.get(termIndex.getIndex()));
            volBucketInfoMap.putIfAbsent(processed.volBucketPair, omBucketInfo);
          }
        }

        for (OzoneManagerProtocolProtos.KeyInfo key :
            path.getDeletedSubFilesList()) {
          ProcessedKeyInfo processed = processDeleteKey(key, path, omMetadataManager);

          omMetrics.decNumKeys();
          numSubFilesMoved++;
          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
              processed.volumeName, processed.bucketName);
          // bucketInfo can be null in case of delete volume or bucket
          // or key does not belong to bucket as bucket is recreated
          if (null != omBucketInfo
              && omBucketInfo.getObjectID() == path.getBucketId()) {
            omBucketInfo.incrUsedBytes(-sumBlockLengths(processed.keyInfo));
            omBucketInfo.incrUsedNamespace(-1L);
            String ozoneDbKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
                path.getBucketId(), processed.keyInfo.getParentObjectID(),
                processed.keyInfo.getFileName());
            omMetadataManager.getFileTable().addCacheEntry(new CacheKey<>(ozoneDbKey),
                CacheValue.get(termIndex.getIndex()));
            volBucketInfoMap.putIfAbsent(processed.volBucketPair, omBucketInfo);
          }
        }
        if (path.hasDeletedDir()) {
          numDirsDeleted++;
        }
      }

      deletingServiceMetrics.incrNumSubDirectoriesMoved(numSubDirMoved);
      deletingServiceMetrics.incrNumSubFilesMoved(numSubFilesMoved);
      deletingServiceMetrics.incrNumDirPurged(numDirsDeleted);

      if (fromSnapshotInfo != null) {
        fromSnapshotInfo.setLastTransactionInfo(TransactionInfo.valueOf(termIndex).toByteString());
        omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(fromSnapshotInfo.getTableKey()),
            CacheValue.get(termIndex.getIndex(), fromSnapshotInfo));
      }

    } catch (IOException ex) {
      // Case of IOException for fromProtobuf will not happen
      // as this is created and send within OM
      // only case of upgrade where compatibility is broken can have
      throw new IllegalStateException(ex);
    } finally {
      for (Map.Entry<Pair<String, String>, OmBucketInfo> entry :
          volBucketInfoMap.entrySet()) {
        entry.setValue(entry.getValue().copyObject());
      }
      omMetadataManager.getLock().releaseWriteLocks(BUCKET_LOCK, bucketLockKeys);
    }

    return new OMDirectoriesPurgeResponseWithFSO(
        omResponse.build(), purgeRequests, ozoneManager.isRatisEnabled(),
            getBucketLayout(), volBucketInfoMap, fromSnapshotInfo);
  }

  /**
   * Helper class to hold processed key information.
   */
  private static class ProcessedKeyInfo {
    private final OmKeyInfo keyInfo;
    private final String deleteKey;
    private final String volumeName;
    private final String bucketName;
    private final Pair<String, String> volBucketPair;

    ProcessedKeyInfo(OmKeyInfo keyInfo, String deleteKey, String volumeName, String bucketName) {
      this.keyInfo = keyInfo;
      this.deleteKey = deleteKey;
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.volBucketPair = Pair.of(volumeName, bucketName);
    }
  }

  /**
   * Process delete key info.
   * Returns ProcessedKeyInfo containing all the processed information.
   */
  private ProcessedKeyInfo processDeleteKey(OzoneManagerProtocolProtos.KeyInfo key,
                                            OzoneManagerProtocolProtos.PurgePathRequest path,
                                            OmMetadataManagerImpl omMetadataManager) throws IOException {
    OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);

    String pathKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
        path.getBucketId(), keyInfo.getParentObjectID(), keyInfo.getFileName());
    String deleteKey = omMetadataManager.getOzoneDeletePathKey(
        keyInfo.getObjectID(), pathKey);

    String volumeName = keyInfo.getVolumeName();
    String bucketName = keyInfo.getBucketName();

    return new ProcessedKeyInfo(keyInfo, deleteKey, volumeName, bucketName);
  }

  private List<String[]> getBucketLockKeySet(PurgeDirectoriesRequest purgeDirsRequest) {
    if (!purgeDirsRequest.getBucketNameInfosList().isEmpty()) {
      return purgeDirsRequest.getBucketNameInfosList().stream()
          .map(keyInfo -> Pair.of(keyInfo.getVolumeName(), keyInfo.getBucketName()))
          .distinct()
          .map(pair -> new String[]{pair.getLeft(), pair.getRight()})
          .collect(Collectors.toList());
    }

    return purgeDirsRequest.getDeletedPathList().stream()
        .flatMap(purgePathRequest -> Stream.concat(purgePathRequest.getDeletedSubFilesList().stream(),
            purgePathRequest.getMarkDeletedSubDirsList().stream()))
        .map(keyInfo -> Pair.of(keyInfo.getVolumeName(), keyInfo.getBucketName()))
        .distinct()
        .map(pair -> new String[]{pair.getLeft(), pair.getRight()})
        .collect(Collectors.toList());
  }

}
