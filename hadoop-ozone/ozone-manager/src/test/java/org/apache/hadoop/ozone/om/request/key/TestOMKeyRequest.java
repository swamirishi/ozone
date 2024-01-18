/**
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.util.Time;
import org.slf4j.event.Level;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.setupReplicationConfigValidation;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test class for key request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMKeyRequest {
  @TempDir
  private Path folder;

  protected OzoneManager ozoneManager;
  protected KeyManager keyManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;
  protected OzoneManagerPrepareState prepareState;

  protected ScmClient scmClient;
  protected OzoneBlockTokenSecretManager ozoneBlockTokenSecretManager;
  protected ScmBlockLocationProtocol scmBlockLocationProtocol;
  protected OMPerformanceMetrics metrics;

  protected static final long CONTAINER_ID = 1000L;
  protected static final long LOCAL_ID = 100L;

  protected String volumeName;
  protected String bucketName;
  protected String keyName;
  protected HddsProtos.ReplicationType replicationType;
  protected HddsProtos.ReplicationFactor replicationFactor;
  protected long clientID;
  protected long scmBlockSize = 1000L;
  protected long dataSize;
  protected Random random;
  protected long txnLogId = 100000L;
  protected long version = 0L;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = getOzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        folder.toAbsolutePath().toString());
    ozoneConfiguration.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    when(ozoneManager.isFilesystemSnapshotEnabled()).thenReturn(true);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(true);
    when(ozoneManager.getBucketInfo(anyString(), anyString())).thenReturn(
        new OmBucketInfo.Builder().setVolumeName("").setBucketName("").build());
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    setupReplicationConfigValidation(ozoneManager, ozoneConfiguration);

    scmClient = mock(ScmClient.class);
    ozoneBlockTokenSecretManager = mock(OzoneBlockTokenSecretManager.class);
    scmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
    metrics = mock(OMPerformanceMetrics.class);
    keyManager = new KeyManagerImpl(ozoneManager, scmClient, ozoneConfiguration,
        metrics);
    when(ozoneManager.getScmClient()).thenReturn(scmClient);
    when(ozoneManager.getBlockTokenSecretManager())
        .thenReturn(ozoneBlockTokenSecretManager);
    when(ozoneManager.getScmBlockSize()).thenReturn(scmBlockSize);
    when(ozoneManager.getPreallocateBlocksMax()).thenReturn(2);
    when(ozoneManager.isGrpcBlockTokenEnabled()).thenReturn(false);
    when(ozoneManager.getOMNodeId()).thenReturn(UUID.randomUUID().toString());
    when(ozoneManager.getOMServiceId()).thenReturn(
        UUID.randomUUID().toString());
    when(scmClient.getBlockClient()).thenReturn(scmBlockLocationProtocol);
    when(ozoneManager.getKeyManager()).thenReturn(keyManager);
    when(ozoneManager.getAccessAuthorizer())
        .thenReturn(new OzoneNativeAuthorizer());

    ReferenceCounted<IOmMetadataReader, SnapshotCache, String> rcOmMetadataReader =
        mock(ReferenceCounted.class);
    when(ozoneManager.getOmMetadataReader()).thenReturn(rcOmMetadataReader);
    // Init OmMetadataReader to let the test pass
    OmMetadataReader omMetadataReader = mock(OmMetadataReader.class);
    when(omMetadataReader.isNativeAuthorizerEnabled()).thenReturn(true);
    when(rcOmMetadataReader.get()).thenReturn(omMetadataReader);

    prepareState = new OzoneManagerPrepareState(ozoneConfiguration);
    when(ozoneManager.getPrepareState()).thenReturn(prepareState);

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setNodes(new ArrayList<>())
        .build();

    AllocatedBlock.Builder blockBuilder = new AllocatedBlock.Builder()
        .setPipeline(pipeline);

    when(scmBlockLocationProtocol.allocateBlock(anyLong(), anyInt(),
        any(ReplicationConfig.class),
        anyString(), any(ExcludeList.class),
        anyString())).thenAnswer(invocation -> {
          int num = invocation.getArgument(1);
          List<AllocatedBlock> allocatedBlocks = new ArrayList<>(num);
          for (int i = 0; i < num; i++) {
            blockBuilder.setContainerBlockID(
                new ContainerBlockID(CONTAINER_ID + i, LOCAL_ID + i));
            allocatedBlocks.add(blockBuilder.build());
          }
          return allocatedBlocks;
        });


    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();
    replicationFactor = HddsProtos.ReplicationFactor.ONE;
    replicationType = HddsProtos.ReplicationType.RATIS;
    clientID = Time.now();
    dataSize = 1000L;
    random = new Random();
    version = 0L;

    ResolvedBucket bucket = new ResolvedBucket(volumeName, bucketName,
        volumeName, bucketName, "owner", BucketLayout.OBJECT_STORE);
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
        any(OMClientRequest.class)))
        .thenReturn(bucket);
    when(ozoneManager.resolveBucketLink(any(Pair.class),
        any(OMClientRequest.class)))
        .thenReturn(bucket);
    when(ozoneManager.resolveBucketLink(any(Pair.class)))
        .thenReturn(bucket);
    OmSnapshotManager omSnapshotManager = new OmSnapshotManager(ozoneManager);
    when(ozoneManager.getOmSnapshotManager())
        .thenReturn(omSnapshotManager);

    // Enable DEBUG level logging for relevant classes
    GenericTestUtils.setLogLevel(OMKeyRequest.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(OMKeyCommitRequest.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(OMKeyCommitRequestWithFSO.LOG, Level.DEBUG);
  }

  @NotNull
  protected OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }


  /**
   * Verify path in open key table. Also, it returns OMKeyInfo for the given
   * key path.
   *
   * @param key      key name
   * @param id       client id
   * @param doAssert if true then do assertion, otherwise it just skip.
   * @return om key info for the given key path.
   * @throws Exception DB failure
   */
  protected OmKeyInfo verifyPathInOpenKeyTable(String key, long id,
                                               boolean doAssert)
      throws Exception {
    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        key, id);
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);
    if (doAssert) {
      assertNotNull(omKeyInfo, "Failed to find key in OpenKeyTable");
    }
    return omKeyInfo;
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }
}
