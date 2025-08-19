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
package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link SendContainerRequestHandler}.
 */
public class TestSendContainerRequestHandler {

  private OzoneConfiguration conf;

  private VolumeChoosingPolicy volumeChoosingPolicy;

  private ContainerSet containerSet;
  private MutableVolumeSet volumeSet;
  private ContainerImporter importer;
  private StreamObserver<ContainerProtos.SendContainerResponse> responseObserver;
  private SendContainerRequestHandler sendContainerRequestHandler;
  private long containerMaxSize;

  @BeforeEach
  void setup() throws IOException {
    conf = new OzoneConfiguration();
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
    containerSet = new ContainerSet(0);
    volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    importer = new ContainerImporter(conf, containerSet,
        mock(ContainerController.class), volumeSet, volumeChoosingPolicy);
    importer = Mockito.spy(importer);
    responseObserver = mock(StreamObserver.class);
    sendContainerRequestHandler = new SendContainerRequestHandler(importer, responseObserver);
    containerMaxSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  @Test
  void testReceiveDataForExistingContainer() throws Exception {
    long containerId = 1;
    // create containerImporter
    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test");
    // add container to container set
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    containerSet.addContainer(container);

    doAnswer(invocation -> {
      Object arg = invocation.getArgument(0);
      Assert.assertTrue(arg instanceof StorageContainerException);
      Assert.assertEquals(((StorageContainerException) arg).getResult(),
          ContainerProtos.Result.CONTAINER_EXISTS);
      return null;
    }).when(responseObserver).onError(any());
    ByteString data = ByteString.copyFromUtf8("test");
    ContainerProtos.SendContainerRequest request
        = ContainerProtos.SendContainerRequest.newBuilder()
        .setContainerID(containerId)
        .setData(data)
        .setOffset(0)
        .setCompression(NO_COMPRESSION.toProto())
        .build();
    sendContainerRequestHandler.onNext(request);
  }

  @Test
  public void testSpaceReservedAndReleasedWhenRequestCompleted() throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();

    // Create request
    ContainerProtos.SendContainerRequest request = ContainerProtos.SendContainerRequest.newBuilder()
        .setContainerID(containerId)
        .setData(ByteString.EMPTY)
        .setOffset(0)
        .setCompression(CopyContainerCompression.NO_COMPRESSION.toProto())
        .build();

    // Execute request
    sendContainerRequestHandler.onNext(request);

    // Verify commit space is reserved
    Assert.assertEquals(volume.getCommittedBytes(), initialCommittedBytes + 2 * containerMaxSize);

    // complete the request
    sendContainerRequestHandler.onCompleted();

    // Verify commit space is released
    Assert.assertEquals(volume.getCommittedBytes(), initialCommittedBytes);
  }

  @Test
  public void testSpaceReservedAndReleasedWhenOnNextFails() throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();

    // Create request
    ContainerProtos.SendContainerRequest request = createRequest(containerId, ByteString.copyFromUtf8("test"), 0);

    // Execute request
    sendContainerRequestHandler.onNext(request);

    // Verify commit space is reserved
    Assert.assertEquals(volume.getCommittedBytes(), initialCommittedBytes + 2 * containerMaxSize);

    // mock the importer is not allowed to import this container
    Mockito.when(importer.isAllowedContainerImport(containerId)).thenReturn(false);

    sendContainerRequestHandler.onNext(request);

    // Verify commit space is released
    Assert.assertEquals(volume.getCommittedBytes(), initialCommittedBytes);
  }

  @Test
  public void testSpaceReservedAndReleasedWhenOnCompletedFails() throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();

    // Create request
    ContainerProtos.SendContainerRequest request = createRequest(containerId, ByteString.copyFromUtf8("test"), 0);

    // Execute request
    sendContainerRequestHandler.onNext(request);

    // Verify commit space is reserved
    Assert.assertEquals(volume.getCommittedBytes(), initialCommittedBytes + 2 * containerMaxSize);

    Mockito.doThrow(new IOException("Failed")).when(importer).importContainer(Mockito.anyLong(), any(), any(), any());

    sendContainerRequestHandler.onCompleted();

    // Verify commit space is released
    Assert.assertEquals(volume.getCommittedBytes(), initialCommittedBytes);
  }

  private ContainerProtos.SendContainerRequest createRequest(long containerId, ByteString data, int offset) {
    return ContainerProtos.SendContainerRequest.newBuilder()
        .setContainerID(containerId)
        .setData(data)
        .setOffset(offset)
        .setCompression(CopyContainerCompression.NO_COMPRESSION.toProto())
        .build();
  }
}
