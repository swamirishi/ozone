/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Class to determine the health state of an EC Container. Given the container
 * and current replica details, along with replicas pending add and delete,
 * this class will return a ContainerHealthResult indicating if the container
 * is healthy, or under / over replicated etc.
 *
 * For EC Containers, it is possible for a container to be both under and over
 * replicated, if there are multiple copies of one index and no copies of
 * another. This class only returns a single status, keeping the container in a
 * single health state at any given time. Under replicated is a more serious
 * state than over replicated, so it will take precedence over any over
 * replication.
 */
public class ECContainerHealthCheck implements ContainerHealthCheck {

  // TODO - mis-replicated containers are not yet handled.
  // TODO - should this class handle empty / deleting containers, or would it
  //        be better handled elsewhere?

  @Override
  public Optional<ContainerHealthResult> checkHealth(ContainerInfo container,
        Set<ContainerReplica> replicas,
        List<ContainerReplicaOp> replicaPendingOps,
        int remainingRedundancyForMaintenance,
        Optional<ContainerHealthResult.HealthState> healthStateFilter) {
    return this.checkHealth(container, replicas, replicaPendingOps,
            remainingRedundancyForMaintenance).stream()
            .filter(containerHealthResult ->
            healthStateFilter.orElse(containerHealthResult.getHealthState())
            == containerHealthResult.getHealthState()).findFirst();
  }

  @Override
  public List<ContainerHealthResult> checkHealth(ContainerInfo container,
      Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> replicaPendingOps,
      int remainingRedundancyForMaintenance) {
    ECContainerReplicaCount replicaCount =
        new ECContainerReplicaCount(container, replicas, replicaPendingOps,
            remainingRedundancyForMaintenance);

    ECReplicationConfig repConfig =
        (ECReplicationConfig) container.getReplicationConfig();
    List<ContainerHealthResult> containerHealthResults = new ArrayList<>();
    if (!replicaCount.isSufficientlyReplicated(false)) {
      List<Integer> missingIndexes = replicaCount.unavailableIndexes(false);
      int remainingRedundancy = repConfig.getParity();
      boolean dueToDecommission = true;
      if (missingIndexes.size() > 0) {
        // The container has reduced redundancy and will need reconstructed
        // via an EC reconstruction command. Note that it may also have some
        // replicas in decommission / maintenance states, but as the under
        // replication is not caused only by decommission, we say it is not
        // due to decommission/
        dueToDecommission = false;
        remainingRedundancy = repConfig.getParity() - missingIndexes.size();
      }
      containerHealthResults.add(
              new ContainerHealthResult.UnderReplicatedHealthResult(
              container, remainingRedundancy, dueToDecommission,
              replicaCount.isSufficientlyReplicated(true),
              replicaCount.isUnrecoverable()));
    }

    if (replicaCount.isOverReplicated(false)) {
      List<Integer> overRepIndexes = replicaCount.overReplicatedIndexes(false);
      containerHealthResults.add(new ContainerHealthResult
          .OverReplicatedHealthResult(container, overRepIndexes.size(),
          !replicaCount.isOverReplicated(true)));
    }

    // If No issues detected, return healthy.
    return containerHealthResults.size() > 0 ? containerHealthResults :
            Collections
            .singletonList(new ContainerHealthResult.HealthyResult(container));
  }
}
