/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.health.ECReplicationCheckHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Handles the EC Under replication processing and forming the respective SCM
 * commands.
 */
public class ECUnderReplicationHandler implements UnhealthyReplicationHandler {

  public static final Logger LOG =
      LoggerFactory.getLogger(ECUnderReplicationHandler.class);
  private final ECReplicationCheckHandler ecReplicationCheck;
  private final PlacementPolicy containerPlacement;
  private final long currentContainerSize;
  private final NodeManager nodeManager;

  public ECUnderReplicationHandler(ECReplicationCheckHandler ecReplicationCheck,
      final PlacementPolicy containerPlacement, final ConfigurationSource conf,
                                   NodeManager nodeManager) {
    this.ecReplicationCheck = ecReplicationCheck;
    this.containerPlacement = containerPlacement;
    this.currentContainerSize = (long) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.nodeManager = nodeManager;
  }

  private boolean validatePlacement(List<DatanodeDetails> replicaNodes,
                                    List<DatanodeDetails> selectedNodes) {
    List<DatanodeDetails> nodes = new ArrayList<>(replicaNodes);
    nodes.addAll(selectedNodes);
    boolean placementStatus = containerPlacement
            .validateContainerPlacement(nodes, nodes.size())
            .isPolicySatisfied();
    if (!placementStatus) {
      LOG.warn("Selected Nodes does not satisfy placement policy: {}. " +
              "Selected nodes: {}. Existing Replica Nodes: {}.",
              containerPlacement.getClass().getName(),
              selectedNodes, replicaNodes);
    }
    return placementStatus;
  }

  /**
   * Identify a new set of datanode(s) to reconstruct the container and form the
   * SCM command to send it to DN. In the case of decommission, it will just
   * generate the replicate commands instead of reconstruction commands.
   *
   * @param replicas - Set of available container replicas.
   * @param pendingOps - Inflight replications and deletion ops.
   * @param result - Health check result.
   * @param remainingMaintenanceRedundancy - represents that how many nodes go
   *                                      into maintenance.
   * @return Returns the key value pair of destination dn where the command gets
   * executed and the command itself. If an empty list is returned, it indicates
   * the container is no longer unhealthy and can be removed from the unhealthy
   * queue. Any exception indicates that the container is still unhealthy and
   * should be retried later.
   */
  @Override
  public Map<DatanodeDetails, SCMCommand<?>> processAndCreateCommands(
      final Set<ContainerReplica> replicas,
      final List<ContainerReplicaOp> pendingOps,
      final ContainerHealthResult result,
      final int remainingMaintenanceRedundancy) throws IOException {
    ContainerInfo container = result.getContainerInfo();
    ECReplicationConfig repConfig =
        (ECReplicationConfig) container.getReplicationConfig();
    final ECContainerReplicaCount replicaCount =
        new ECContainerReplicaCount(container, replicas, pendingOps,
            remainingMaintenanceRedundancy);
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setContainerInfo(container)
        .setContainerReplicas(replicas)
        .setPendingOps(pendingOps)
        .setMaintenanceRedundancy(remainingMaintenanceRedundancy)
        .build();
    ContainerHealthResult currentUnderRepRes = ecReplicationCheck
        .checkHealth(request);
    LOG.debug("Handling under-replicated EC container: {}", container);
    if (currentUnderRepRes
        .getHealthState() != ContainerHealthResult.HealthState
        .UNDER_REPLICATED) {
      LOG.info("The container {} state changed and it's not in under"
              + " replication any more. Current state is: {}",
          container.getContainerID(), currentUnderRepRes);
      return emptyMap();
    }
    // don't place reconstructed replicas on exclude nodes, since they already
    // have replicas
    List<DatanodeDetails> excludedNodes = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());

    ContainerHealthResult.UnderReplicatedHealthResult containerHealthResult =
        ((ContainerHealthResult.UnderReplicatedHealthResult)
            currentUnderRepRes);
    if (containerHealthResult.isSufficientlyReplicatedAfterPending() &&
            containerHealthResult.getPlacementStatus().isPolicySatisfied()) {
      LOG.info("The container {} with replicas {} is sufficiently replicated",
          container.getContainerID(), replicaCount.getReplicas());
      return emptyMap();
    }
    if (replicaCount.isUnrecoverable()) {
      LOG.warn("The container {} is unrecoverable. The available replicas" +
          " are: {}.", container.containerID(), replicaCount.getReplicas());
      return emptyMap();
    }
    final ContainerID id = container.containerID();
    final Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
    try {
     // State is UNDER_REPLICATED
      final List<DatanodeDetails> deletionInFlight = new ArrayList<>();
      for (ContainerReplicaOp op : pendingOps) {
        if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
          deletionInFlight.add(op.getTarget());
        }
      }
      List<Integer> missingIndexes = replicaCount.unavailableIndexes(true);
      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources =
              filterSources(replicas, deletionInFlight);
      List<DatanodeDetails> nodes =
              sources.values().stream().map(Pair::getLeft)
                      .map(ContainerReplica::getDatanodeDetails)
                      .filter(datanodeDetails ->
                              !datanodeDetails.isDecomissioned())
                      .collect(Collectors.toList());
      // We got the missing indexes, this is excluded any decommissioning
      // indexes. Find the good source nodes.
      Set<Integer> createdIndexes = new HashSet<>();
      if (missingIndexes.size() > 0) {

        LOG.debug("Missing indexes detected for the container {}." +
                " The missing indexes are {}", id, missingIndexes);
        // We have source nodes.
        if (sources.size() >= repConfig.getData()) {
          final List<DatanodeDetails> selectedDatanodes = getTargetDatanodes(
              excludedNodes, container, missingIndexes.size());

          if (validatePlacement(nodes, selectedDatanodes)) {
            excludedNodes.addAll(selectedDatanodes);
            nodes.addAll(selectedDatanodes);
            List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
                    sourceDatanodesWithIndex = new ArrayList<>();
            for (Pair<ContainerReplica, NodeStatus> src : sources.values()) {
              sourceDatanodesWithIndex.add(
                      new ReconstructECContainersCommand
                              .DatanodeDetailsAndReplicaIndex(
                              src.getLeft().getDatanodeDetails(),
                              src.getLeft().getReplicaIndex()));
            }

            final ReconstructECContainersCommand reconstructionCommand =
                    new ReconstructECContainersCommand(id.getProtobuf().getId(),
                            sourceDatanodesWithIndex, selectedDatanodes,
                            int2byte(missingIndexes),
                            repConfig);
            // Keeping the first target node as coordinator.
            commands.put(selectedDatanodes.get(0), reconstructionCommand);
            createdIndexes.addAll(missingIndexes);
          }
        } else {
          LOG.warn("Cannot proceed for EC container reconstruction for {}, due"
              + " to insufficient source replicas found. Number of source "
              + "replicas needed: {}. Number of available source replicas are:"
              + " {}. Available sources are: {}", container.containerID(),
              repConfig.getData(), sources.size(), sources);
        }
      }
      Set<Integer> decomIndexes = replicaCount.decommissioningOnlyIndexes(true);
      if (decomIndexes.size() > 0) {
        final List<DatanodeDetails> selectedDatanodes =
            getTargetDatanodes(excludedNodes, container, decomIndexes.size());
        if (validatePlacement(nodes, selectedDatanodes)) {
          excludedNodes.addAll(selectedDatanodes);
          Iterator<DatanodeDetails> iterator = selectedDatanodes.iterator();
          // In this case we need to do one to one copy.
          for (ContainerReplica replica : replicas) {
            if (decomIndexes.contains(replica.getReplicaIndex())) {
              if (!iterator.hasNext()) {
                LOG.warn("Couldn't find enough targets. Available source"
                    + " nodes: {}, the target nodes: {}, excluded nodes: {} and"
                    + "  the decommission indexes: {}",
                    replicas, selectedDatanodes, excludedNodes, decomIndexes);
                break;
              }
              DatanodeDetails decommissioningSrcNode
                  = replica.getDatanodeDetails();
              final ReplicateContainerCommand replicateCommand =
                  new ReplicateContainerCommand(id.getProtobuf().getId(),
                      ImmutableList.of(decommissioningSrcNode));
              // For EC containers, we need to track the replica index which is
              // to be replicated, so add it to the command.
              replicateCommand.setReplicaIndex(replica.getReplicaIndex());
              DatanodeDetails target = iterator.next();
              commands.put(target, replicateCommand);
              decomIndexes.remove(replica.getReplicaIndex());
              createdIndexes.add(replica.getReplicaIndex());
            }
          }
        }
      }
      processMaintenanceOnlyIndexes(replicaCount, replicas, excludedNodes,
          commands, createdIndexes);
      List<Integer> sortedReplicaIdx =
              getSourcesStream(replicas, deletionInFlight)
              .collect(Collectors.groupingBy(
                      r->r.getLeft().getReplicaIndex(),Collectors.counting()))
              .entrySet().stream()
              .sorted(Comparator.comparingLong(Map.Entry::getValue))
              .map(Map.Entry::getKey).collect(Collectors.toList());
      processMisreplication(sortedReplicaIdx, container, commands,
              excludedNodes, sources, createdIndexes,
              containerHealthResult.getPlacementStatus());
    } catch (IOException | IllegalStateException ex) {
      LOG.warn("Exception while processing for creating the EC reconstruction" +
              " container commands for {}.",
          id, ex);
      throw ex;
    }
    if (commands.size() == 0) {
      LOG.warn("Container {} is under replicated, but no commands were " +
          "created to correct it", id);
    }
    return commands;
  }

  private Stream<Pair<ContainerReplica, NodeStatus>> getSourcesStream(
          Set<ContainerReplica> replicas,
          List<DatanodeDetails> deletionInFlight) {
    return replicas.stream().filter(r -> r
                    .getState() == StorageContainerDatanodeProtocolProtos
                    .ContainerReplicaProto.State.CLOSED)
            // Exclude stale and dead nodes. This is particularly important for
            // maintenance nodes, as the replicas will remain present in the
            // container manager, even when they go dead.
            .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
            .map(r -> Pair.of(r, ReplicationManager
                    .getNodeStatus(r.getDatanodeDetails(), nodeManager)))
            .filter(pair -> pair.getRight().isHealthy());
  }

  private Map<Integer, Pair<ContainerReplica, NodeStatus>> filterSources(
      Set<ContainerReplica> replicas, List<DatanodeDetails> deletionInFlight) {
    return getSourcesStream(replicas, deletionInFlight)
    // If there are multiple nodes online for a given index, we just
    // pick any IN_SERVICE one. At the moment, the input streams cannot
    // handle multiple replicas for the same index, so if we passed them
    // all through they would not get used anyway.
    // If neither of the nodes are in service, we just pass one through,
    // as it will be decommission or maintenance.
        .collect(Collectors.toMap(
            pair -> pair.getLeft().getReplicaIndex(),
            pair -> pair,
            (p1, p2) -> {
              if (p1.getRight().getOperationalState() == IN_SERVICE) {
                return p1;
              } else {
                return p2;
              }
            }));
  }

  private List<DatanodeDetails> getTargetDatanodes(
      List<DatanodeDetails> excludedNodes, ContainerInfo container,
      int requiredNodes) throws IOException {
    // We should ensure that the target datanode has enough space
    // for a complete container to be created, but since the container
    // size may be changed smaller than origin, we should be defensive.
    final long dataSizeRequired =
        Math.max(container.getUsedBytes(), currentContainerSize);
    return containerPlacement
        .chooseDatanodes(excludedNodes, null, requiredNodes, 0,
            dataSizeRequired);
  }

  /**
   * Processes replicas that are in maintenance nodes and should need
   * additional copies.
   * @param replicaCount
   * @param replicas set of container replicas
   * @param excludedNodes nodes that should not be targets for new copies
   * @param commands
   * @throws IOException
   */
  private void processMaintenanceOnlyIndexes(
      ECContainerReplicaCount replicaCount, Set<ContainerReplica> replicas,
      List<DatanodeDetails> excludedNodes,
      Map<DatanodeDetails, SCMCommand<?>> commands,
      Set<Integer> createdIndexes) throws IOException {
    Set<Integer> maintIndexes = replicaCount.maintenanceOnlyIndexes(true);
    if (maintIndexes.isEmpty()) {
      return;
    }

    ContainerInfo container = replicaCount.getContainer();
    // this many maintenance replicas need another copy
    int additionalMaintenanceCopiesNeeded =
        replicaCount.additionalMaintenanceCopiesNeeded(true);
    List<DatanodeDetails> targets = getTargetDatanodes(excludedNodes, container,
        additionalMaintenanceCopiesNeeded);
    excludedNodes.addAll(targets);

    Iterator<DatanodeDetails> iterator = targets.iterator();
    // copy replica from source maintenance DN to a target DN
    for (ContainerReplica replica : replicas) {
      if (maintIndexes.contains(replica.getReplicaIndex()) &&
          additionalMaintenanceCopiesNeeded > 0) {
        if (!iterator.hasNext()) {
          LOG.warn("Couldn't find enough targets. Available source"
                  + " nodes: {}, target nodes: {}, excluded nodes: {} and"
                  + " maintenance indexes: {}",
              replicas, targets, excludedNodes, maintIndexes);
          break;
        }
        DatanodeDetails maintenanceSourceNode = replica.getDatanodeDetails();
        final ReplicateContainerCommand replicateCommand =
            new ReplicateContainerCommand(
                container.containerID().getProtobuf().getId(),
                ImmutableList.of(maintenanceSourceNode));
        // For EC containers we need to track the replica index which is
        // to be replicated, so add it to the command.
        replicateCommand.setReplicaIndex(replica.getReplicaIndex());
        DatanodeDetails target = iterator.next();
        commands.put(target, replicateCommand);
        additionalMaintenanceCopiesNeeded -= 1;
        maintIndexes.remove(replica.getReplicaIndex());
        createdIndexes.add(replica.getReplicaIndex());
      }
    }
  }

  /**
   * Function processes Replicas to fix placement policy issues.
   * @param sortedSourceReplicatedIndex
   * @param container
   * @param commands
   * @param excludedNodes
   * @param sources
   * @param createdIndexes
   * @param placementStatus
   * @throws IOException
   */
  private void processMisreplication(List<Integer> sortedSourceReplicatedIndex,
      ContainerInfo container,
      Map<DatanodeDetails, SCMCommand<?>> commands,
      List<DatanodeDetails> excludedNodes,
      Map<Integer, Pair<ContainerReplica, NodeStatus>> sources,
      Set<Integer> createdIndexes, ContainerPlacementStatus placementStatus)
          throws IOException   {
    if (placementStatus.isPolicySatisfied() ||
            placementStatus.misReplicationCount()<=createdIndexes.size()) {
      return;
    }
    int additionalCopiesForPlacementStatus =
            placementStatus.misReplicationCount() - createdIndexes.size();
    List<DatanodeDetails> targets = getTargetDatanodes(excludedNodes, container,
            additionalCopiesForPlacementStatus);
    excludedNodes.addAll(targets);

    Iterator<DatanodeDetails> iterator = targets.iterator();
    // copy replica from source DN to a target DN
    for (int replicaIdx : sortedSourceReplicatedIndex) {
      if (!createdIndexes.contains(replicaIdx) &&
              additionalCopiesForPlacementStatus > 0) {
        if (!iterator.hasNext()) {
          LOG.warn("Couldn't find enough targets to fix placement status. "
                   + "Available source target nodes: {}, excluded nodes: {}"
                   + " and additional Nodes Required: {}",
                  targets, excludedNodes, additionalCopiesForPlacementStatus);
          break;
        }
        DatanodeDetails sourceNode = sources.get(replicaIdx)
                .getKey().getDatanodeDetails();
        final ReplicateContainerCommand replicateCommand =
                new ReplicateContainerCommand(
                        container.containerID().getProtobuf().getId(),
                        ImmutableList.of(sourceNode));
        // For EC containers we need to track the replica index which is
        // to be replicated, so add it to the command.
        replicateCommand.setReplicaIndex(replicaIdx);
        DatanodeDetails target = iterator.next();
        commands.put(target, replicateCommand);
        additionalCopiesForPlacementStatus -= 1;
        createdIndexes.add(replicaIdx);
      }
    }
  }

  private static byte[] int2byte(List<Integer> src) {
    byte[] dst = new byte[src.size()];

    for (int i = 0; i < src.size(); i++) {
      dst[i] = src.get(i).byteValue();
    }
    return dst;
  }
}
