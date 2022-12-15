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
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Handles the Mis replication processing and forming the respective SCM
 * commands.
 */
public abstract class MisReplicationHandler implements
        UnhealthyReplicationHandler {

  public static final Logger LOG =
          LoggerFactory.getLogger(MisReplicationHandler.class);
  private final PlacementPolicy<ContainerReplica> containerPlacement;
  private final long currentContainerSize;
  private final NodeManager nodeManager;

  public MisReplicationHandler(
          final PlacementPolicy<ContainerReplica> containerPlacement,
          final ConfigurationSource conf, NodeManager nodeManager) {
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf.getStorageSize(
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.nodeManager = nodeManager;
  }

  protected abstract ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo containerInfo, Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> pendingOps, int remainingMaintenanceRedundancy)
      throws IOException;

  private List<DatanodeDetails> getTargetDatanodes(
          List<DatanodeDetails> usedNodes, List<DatanodeDetails> excludedNodes,
          ContainerInfo container, int requiredNodes) throws IOException {
    final long dataSizeRequired =
            Math.max(container.getUsedBytes(), currentContainerSize);
    while (requiredNodes > 0) {
      try {
        return containerPlacement.chooseDatanodes(usedNodes, excludedNodes,
                null, requiredNodes, 0, dataSizeRequired);
      } catch (IOException e) {
        requiredNodes -= 1;
      }
    }
    throw new SCMException(String.format("Placement Policy: %s did not return"
                    + " any number of nodes. Number of required "
                    + "Nodes %d, Datasize Required: %d",
            containerPlacement.getClass(), requiredNodes, dataSizeRequired),
            SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
  }

  private Set<ContainerReplica> filterSources(Set<ContainerReplica> replicas) {
    return replicas.stream().filter(r -> r
                    .getState() == StorageContainerDatanodeProtocolProtos
                    .ContainerReplicaProto.State.CLOSED)
            .filter(r -> ReplicationManager
                    .getNodeStatus(r.getDatanodeDetails(), nodeManager)
                    .isHealthy())
            .filter(r -> r.getDatanodeDetails().getPersistedOpState()
                    == HddsProtos.NodeOperationalState.IN_SERVICE)
            .collect(Collectors.toSet());
  }

  protected abstract ReplicateContainerCommand getReplicateCommand(
          ContainerInfo containerInfo, ContainerReplica replica);

  private Map<DatanodeDetails, SCMCommand<?>> getReplicateCommands(
          ContainerInfo containerInfo,
          Set<ContainerReplica> replicasToBeReplicated,
          List<DatanodeDetails> targetDns) {
    Map<DatanodeDetails, SCMCommand<?>> commandMap = Maps.newHashMap();
    int datanodeIdx = 0;
    for (ContainerReplica replica : replicasToBeReplicated) {
      if (datanodeIdx == targetDns.size()) {
        break;
      }
      commandMap.put(targetDns.get(datanodeIdx),
              getReplicateCommand(containerInfo, replica));
      datanodeIdx += 1;
    }
    return commandMap;

  }
  @Override
  public Map<DatanodeDetails, SCMCommand<?>> processAndCreateCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int remainingMaintenanceRedundancy)
      throws IOException {
    ContainerInfo container = result.getContainerInfo();
    if (!pendingOps.isEmpty()) {
      LOG.info("Skipping Mis-Replication for Container {}, " +
               "as there are still some pending ops for the container: {}",
              container, pendingOps);
      return Collections.emptyMap();
    }
    ContainerReplicaCount replicaCount = getContainerReplicaCount(container,
            replicas, Collections.emptyList(), remainingMaintenanceRedundancy);

    if (!replicaCount.isSufficientlyReplicated() ||
            replicaCount.isOverReplicated()) {
      LOG.info("Container {} state should be neither under replicated " +
              "nor over replicated before resolving misreplication." +
              "Container UnderReplication status: {}," +
              "Container OverReplication status: {}",
              container.getContainerID(),
              !replicaCount.isSufficientlyReplicated(),
              replicaCount.isOverReplicated());
      return Collections.emptyMap();
    }

    List<DatanodeDetails> usedDns = replicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());
    if (containerPlacement.validateContainerPlacement(usedDns,
            usedDns.size()).isPolicySatisfied()) {
      LOG.info("Container {} is currently not misreplicated",
              container.getContainerID());
      return Collections.emptyMap();
    }

    Set<ContainerReplica> sources = filterSources(replicas);
    Set<ContainerReplica> replicasToBeReplicated = containerPlacement
            .replicasToCopyToFixMisreplication(replicas.stream()
            .collect(Collectors.toMap(Function.identity(), sources::contains)));
    usedDns = replicas.stream().filter(r -> !replicasToBeReplicated.contains(r))
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());
    List<DatanodeDetails> excludedDns = replicasToBeReplicated.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());
    List<DatanodeDetails> targetDatanodes = getTargetDatanodes(usedDns,
           excludedDns, container, replicasToBeReplicated.size());
    if (targetDatanodes.size() < replicasToBeReplicated.size()) {
      LOG.warn("Placement Policy {} found only {} nodes for Container: {}," +
               " number of required nodes: {}, usedNodes : {}",
              containerPlacement.getClass(), targetDatanodes.size(),
              container.getContainerID(), replicasToBeReplicated.size(),
              usedDns);
    }
    return getReplicateCommands(container, replicasToBeReplicated,
            targetDatanodes);
  }
}
