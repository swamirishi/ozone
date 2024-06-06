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

package org.apache.hadoop.ozone.container;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.apache.hadoop.ozone.container.TestHelper.waitForReplicaCount;
import static org.apache.ozone.test.GenericTestUtils.setLogLevel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.event.Level;

/**
 * Tests ozone containers replication.
 */
@Timeout(300)
class TestContainerReplication {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";

  private static final List<Class<? extends PlacementPolicy>> POLICIES = asList(
      SCMContainerPlacementCapacity.class,
      SCMContainerPlacementRackAware.class,
      SCMContainerPlacementRandom.class
  );

  static List<Arguments> containerReplicationArguments() {
    List<Arguments> arguments = new LinkedList<>();
    for (Class<? extends PlacementPolicy> policyClass : POLICIES) {
      String canonicalName = policyClass.getCanonicalName();
      arguments.add(Arguments.arguments(canonicalName, true));
      arguments.add(Arguments.arguments(canonicalName, false));
    }
    return arguments;
  }

  @BeforeAll
  static void setUp() {
    setLogLevel(SCMContainerPlacementCapacity.LOG, Level.DEBUG);
    setLogLevel(SCMContainerPlacementRackAware.LOG, Level.DEBUG);
    setLogLevel(SCMContainerPlacementRandom.LOG, Level.DEBUG);
  }

  @ParameterizedTest
  @MethodSource("containerReplicationArguments")
  void testContainerReplication(
      String placementPolicyClass, boolean legacyEnabled) throws Exception {

    OzoneConfiguration conf = createConfiguration(legacyEnabled);
    conf.set(OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY, placementPolicyClass);
    try (MiniOzoneCluster cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();
      try (OzoneClient client = cluster.newClient()) {
        createTestData(client);

        List<OmKeyLocationInfo> keyLocations = lookupKey(cluster);
        assertThat(keyLocations).isNotEmpty();

        OmKeyLocationInfo keyLocation = keyLocations.get(0);
        long containerID = keyLocation.getContainerID();
        waitForContainerClose(cluster, containerID);

        cluster.shutdownHddsDatanode(keyLocation.getPipeline().getFirstNode());
        waitForReplicaCount(containerID, 2, cluster);

        waitForReplicaCount(containerID, 3, cluster);
      }
    }
  }

  private static MiniOzoneCluster newCluster(OzoneConfiguration conf)
      throws IOException {
    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
  }

  private static OzoneConfiguration createConfiguration(boolean enableLegacy) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setEnableLegacy(enableLegacy);
    repConf.setInterval(Duration.ofSeconds(1));
    repConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);

    return conf;
  }

  // TODO use common helper to create test data
  private void createTestData(OzoneClient client) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);

    OzoneBucket bucket = volume.getBucket(BUCKET);

    try (OutputStream out = bucket.createKey(KEY, 0,
        RatisReplicationConfig.getInstance(THREE), emptyMap())) {
      out.write("Hello".getBytes(UTF_8));
    }
  }

  private byte[] createTestData(OzoneClient client, int size) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);
    OzoneBucket bucket = volume.getBucket(BUCKET);
    try (OutputStream out = bucket.createKey(KEY, 0, new ECReplicationConfig("RS-3-2-1k"),
        new HashMap<>())) {
      byte[] b = new byte[size];
      new Random().nextBytes(b);
      out.write(b);
      return b;
    }
  }

  private static List<OmKeyLocationInfo> lookupKey(MiniOzoneCluster cluster)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    assertNotNull(locations);
    return locations.getLocationList();
  }

  private static OmKeyLocationInfo lookupKeyFirstLocation(MiniOzoneCluster cluster)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    Assertions.assertNotNull(locations);
    return locations.getLocationList().get(0);
  }


  public void assertState(MiniOzoneCluster cluster, Map<Integer, DatanodeDetails> expectedReplicaMap)
      throws IOException {
    OmKeyLocationInfo keyLocation = lookupKeyFirstLocation(cluster);
    Map<Integer, DatanodeDetails> replicaMap =
        keyLocation.getPipeline().getNodes().stream().collect(Collectors.toMap(
            dn -> keyLocation.getPipeline().getReplicaIndex(dn), Functions.identity()));
    Assertions.assertEquals(expectedReplicaMap, replicaMap);
  }

  private OzoneInputStream createInputStream(OzoneClient client) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    OzoneBucket bucket = volume.getBucket(BUCKET);
    return bucket.readKey(KEY);
  }


  @Test
  public void testECContainerReplication() throws Exception {
    OzoneConfiguration conf = createConfiguration(false);
    final AtomicReference<DatanodeDetails> mockedDatanodeToRemove = new AtomicReference<>();

    // Overiding Config to support 1k Chunk size
    conf.set("ozone.replication.allowed-configs", "(^((STANDALONE|RATIS)/(ONE|THREE))|(EC/(3-2|6-3|10-4)-" +
        "(512|1024|2048|4096|1)k)$)");
    conf.set(OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY, SCMContainerPlacementRackScatter.class.getCanonicalName());
    try (MockedStatic<ContainerPlacementPolicyFactory> mocked =
             Mockito.mockStatic(ContainerPlacementPolicyFactory.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(() -> ContainerPlacementPolicyFactory.getECPolicy(any(ConfigurationSource.class),
          any(NodeManager.class), any(NetworkTopology.class), Mockito.anyBoolean(),
          any(SCMContainerPlacementMetrics.class))).thenAnswer(i -> {
            PlacementPolicy placementPolicy = (PlacementPolicy) Mockito.spy(i.callRealMethod());
            Mockito.doAnswer(args -> {
              Set<ContainerReplica> containerReplica = ((Set<ContainerReplica>) args.getArgument(0)).stream()
                  .filter(r -> r.getDatanodeDetails().equals(mockedDatanodeToRemove.get()))
                  .collect(Collectors.toSet());
              return containerReplica;
            }).when(placementPolicy).replicasToRemoveToFixOverreplication(Mockito.anySet(), Mockito.anyInt());
            return placementPolicy;
          });

      // Creating Cluster with 6 Nodes
      try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(6).build()) {
        cluster.waitForClusterToBeReady();
        try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
          Set<DatanodeDetails> allNodes =
              cluster.getHddsDatanodes().stream().map(HddsDatanodeService::getDatanodeDetails).collect(
                  Collectors.toSet());
          List<DatanodeDetails> initialNodesWithData = new ArrayList<>();
          DatanodeDetails extraNode = null;
          // Keeping 5 DNs and stopping the 6th Node here it is kept in the var extraNode
          for (DatanodeDetails dn : allNodes) {
            if (initialNodesWithData.size() < 5) {
              initialNodesWithData.add(dn);
            } else {
              extraNode = dn;
              cluster.shutdownHddsDatanode(dn);
            }
          }

          // Creating 2 stripes with Chunk Size 1k
          int size = 6 * 1024;
          byte[] originalData = createTestData(client, size);

          // Getting latest location of the key
          final OmKeyLocationInfo keyLocation = lookupKeyFirstLocation(cluster);
          long containerID = keyLocation.getContainerID();
          waitForContainerClose(cluster, containerID);

          // Forming Replica Index Map
          Map<Integer, DatanodeDetails> replicaIndexMap =
              initialNodesWithData.stream().map(dn -> new Object[]{dn, keyLocation.getPipeline().getReplicaIndex(dn)})
                  .collect(
                      Collectors.toMap(x -> (Integer) x[1], x -> (DatanodeDetails) x[0]));

          //Reading through file and comparing with input data.
          byte[] readData = new byte[size];
          try (OzoneInputStream inputStream = createInputStream(client)) {
            inputStream.read(readData);
            Assertions.assertTrue(Arrays.equals(readData, originalData));
          }

          //Opening a new stream before we make changes to the blocks.
          try (OzoneInputStream inputStream = createInputStream(client)) {
            int firstReadLen = 1024 * 3;
            Arrays.fill(readData, (byte)0);
            inputStream.read(readData, 0, firstReadLen);
            //Checking the initial state as per the latest location.
            assertState(cluster, ImmutableMap.of(1, replicaIndexMap.get(1), 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(3), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            // Shutting down DN1 and waiting for underreplication
            cluster.shutdownHddsDatanode(replicaIndexMap.get(1));
            waitForReplicaCount(containerID, 4, cluster);
            assertState(cluster, ImmutableMap.of(2, replicaIndexMap.get(2), 3, replicaIndexMap.get(3),
                4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            //Starting up ExtraDN. RM should run and create Replica Index 1 to ExtraDN
            cluster.restartHddsDatanode(extraNode, false);
            waitForReplicaCount(containerID, 5, cluster);
            assertState(cluster, ImmutableMap.of(1, extraNode, 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(3), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            //Stopping RM and starting DN1, this should lead to overreplication of ReplicaIndex 1
            cluster.getStorageContainerManager().getReplicationManager().stop();
            cluster.restartHddsDatanode(replicaIndexMap.get(1), true);
            waitForReplicaCount(containerID, 6, cluster);

            //Mocking Overreplication processor to remove replica from DN1. Final Replica1 should be in extraNode
            mockedDatanodeToRemove.set(replicaIndexMap.get(1));
            cluster.getStorageContainerManager().getReplicationManager().start();
            waitForReplicaCount(containerID, 5, cluster);
            assertState(cluster, ImmutableMap.of(1, extraNode, 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(3), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            //Stopping DN3 and waiting for underreplication
            cluster.getStorageContainerManager().getReplicationManager().stop();
            cluster.shutdownHddsDatanode(replicaIndexMap.get(3));
            waitForReplicaCount(containerID, 4, cluster);
            assertState(cluster, ImmutableMap.of(1, extraNode, 2, replicaIndexMap.get(2),
                4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            //Starting RM, Under replication processor should create Replica 3 in DN1
            cluster.getStorageContainerManager().getReplicationManager().start();
            waitForReplicaCount(containerID, 5, cluster);
            assertState(cluster, ImmutableMap.of(1, extraNode, 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(1), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            //Starting DN3 leading to overreplication of replica 3
            cluster.getStorageContainerManager().getReplicationManager().stop();
            cluster.restartHddsDatanode(replicaIndexMap.get(3), true);
            waitForReplicaCount(containerID, 6, cluster);

            //Mocking Overreplication processor to remove data from DN3 leading to Replica3 to stay in DN1.
            mockedDatanodeToRemove.set(replicaIndexMap.get(3));
            cluster.getStorageContainerManager().getReplicationManager().start();
            waitForReplicaCount(containerID, 5, cluster);
            assertState(cluster, ImmutableMap.of(1, extraNode, 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(1), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            //Stopping Extra DN leading to underreplication of Replica 1
            cluster.getStorageContainerManager().getReplicationManager().stop();
            cluster.shutdownHddsDatanode(extraNode);
            waitForReplicaCount(containerID, 4, cluster);
            assertState(cluster, ImmutableMap.of(2, replicaIndexMap.get(2), 3, replicaIndexMap.get(1),
                4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));


            //RM should fix underreplication and write data to DN3
            cluster.getStorageContainerManager().getReplicationManager().start();
            waitForReplicaCount(containerID, 5, cluster);
            assertState(cluster, ImmutableMap.of(1, replicaIndexMap.get(3), 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(1), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            // Reading the pre initialized inputStream. This leads to swap of the block1 & block3.
            inputStream.read(readData, firstReadLen, size - firstReadLen);
            Assertions.assertTrue(Arrays.equals(readData, originalData));
          }
        }
      }
    }
  }

}
