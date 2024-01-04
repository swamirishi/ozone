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

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assume.assumeTrue;

/**
 * Tests OmKeyDelete request.
 */
@RunWith(Parameterized.class)
public class TestOMKeyDeleteRequest extends TestOMKeyRequest {

  private final String testKeyName1;
  private final String testKeyName2;
  private final String expectedExceptionMessage;

  public TestOMKeyDeleteRequest(String testKeyName1, String testKeyName2, String expectedExceptionMessage) {
    this.testKeyName1 = testKeyName1;
    this.testKeyName2 = testKeyName2;
    this.expectedExceptionMessage = expectedExceptionMessage;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"keyName", ".snapshot", "Cannot delete key with reserved name: .snapshot"},
        {"a/b/keyName", ".snapshot/snapName", "Cannot delete key under path reserved for snapshot: .snapshot/"},
        {"a/.snapshot/keyName", ".snapshot/snapName/keyName",
            "Cannot delete key under path reserved for snapshot: .snapshot/" },
        {"a.snapshot/b/keyName", null, null }
    });
  }

  @Test
  public void testPreExecute() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable(testKeyName1);
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    Assert.assertNotNull(omKeyInfo);

    doPreExecute(createDeleteKeyRequest(testKeyName1));
  }

  @Test
  public void testPreExecuteFailure() {
    assumeTrue(testKeyName2 != null && expectedExceptionMessage != null);
    OMKeyDeleteRequest deleteKeyRequest =
        getOmKeyDeleteRequest(createDeleteKeyRequest(testKeyName2));
    OMException omException = Assert.assertThrows(OMException.class,
        () -> deleteKeyRequest.preExecute(ozoneManager));
    Assert.assertEquals(expectedExceptionMessage, omException.getMessage());
    Assert.assertEquals(OMException.ResultCodes.INVALID_KEY_NAME, omException.getResult());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    Assert.assertNotNull(omKeyInfo);

    OMRequest modifiedOmRequest =
            doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
            getOmKeyDeleteRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    // Now after calling validateAndUpdateCache, it should be deleted.

    omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {
    // Add only volume and bucket entry to DB.
    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMKeyDeleteRequest omKeyDeleteRequest =
        getOmKeyDeleteRequest(createDeleteKeyRequest());

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMKeyDeleteRequest omKeyDeleteRequest = getOmKeyDeleteRequest(
        createDeleteKeyRequest());

    OMClientResponse omClientResponse = omKeyDeleteRequest
        .validateAndUpdateCache(ozoneManager, TransactionInfo.getTermIndex(100L),
            omKeyDeleteRequest.getBucketLayout());

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    OMKeyDeleteRequest omKeyDeleteRequest =
            getOmKeyDeleteRequest(createDeleteKeyRequest());

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMClientResponse omClientResponse = omKeyDeleteRequest
        .validateAndUpdateCache(ozoneManager, TransactionInfo.getTermIndex(100L),
            omKeyDeleteRequest.getBucketLayout());

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
            omClientResponse.getOMResponse().getStatus());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  protected OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {

    OMKeyDeleteRequest omKeyDeleteRequest =
            getOmKeyDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omKeyDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates DeleteKeyRequest.
   * @return OMRequest
   */
  protected OMRequest createDeleteKeyRequest() {
    return createDeleteKeyRequest(keyName);
  }

  private OMRequest createDeleteKeyRequest(String testKeyName) {
    KeyArgs keyArgs = KeyArgs.newBuilder().setBucketName(bucketName)
        .setVolumeName(volumeName).setKeyName(testKeyName).build();

    DeleteKeyRequest deleteKeyRequest =
        DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder().setDeleteKeyRequest(deleteKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  protected String addKeyToTable() throws Exception {
    return addKeyToTable(keyName);
  }

  protected String addKeyToTable(String key) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, volumeName,
            bucketName, key, clientID, replicationType, replicationFactor,
            omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName, key);
  }

  protected OMKeyDeleteRequest getOmKeyDeleteRequest(
      OMRequest modifiedOmRequest) {
    return new OMKeyDeleteRequest(modifiedOmRequest, BucketLayout.DEFAULT);
  }
}
