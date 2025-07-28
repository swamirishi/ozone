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
package org.apache.hadoop.hdds.scm.container.replication;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests for ContainerReplicaPendingOps.
 */
public class TestContainerReplicaPendingOps {

  private ContainerReplicaPendingOps pendingOps;
  private TestClock clock;
  private DatanodeDetails dn1;
  private DatanodeDetails dn2;
  private DatanodeDetails dn3;
  private ReplicationManagerMetrics metrics;
  private long deadline;
  private SCMCommand<?> addCmd;
  private SCMCommand<?> deleteCmd;
  private static final long FIVE_GB_CONTAINER_SIZE = 5L * 1024 * 1024 * 1024;
  private static final long THREE_GB_CONTAINER_SIZE = 3L * 1024 * 1024 * 1024;
  private ReplicationManager.ReplicationManagerConfiguration rmConf;

  @BeforeEach
  public void setup() {
    clock = new TestClock(Instant.now(), ZoneOffset.UTC);
    deadline = clock.millis() + 10000; // Current time plus 10 seconds

    OzoneConfiguration conf = new OzoneConfiguration();
    rmConf = conf.getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    ReplicationManager rm = mock(ReplicationManager.class);
    Mockito.when(rm.getConfig()).thenReturn(rmConf);
    pendingOps = new ContainerReplicaPendingOps(clock, rmConf);
    metrics = ReplicationManagerMetrics.create(rm);
    pendingOps.setReplicationMetrics(metrics);
    dn1 = MockDatanodeDetails.randomDatanodeDetails();
    dn2 = MockDatanodeDetails.randomDatanodeDetails();
    dn3 = MockDatanodeDetails.randomDatanodeDetails();

    addCmd = ReplicateContainerCommand.toTarget(1, dn3);
    deleteCmd =  new DeleteContainerCommand(1, false);
  }

  @Test
  public void testGetPendingOpsReturnsEmptyList() {
    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(0, ops.size());
  }

  @Test
  public void testClear() {
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn1, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(2), dn1, 0, deleteCmd, deadline);

    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));

    pendingOps.clear();

    Assertions.assertEquals(0,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    Assertions.assertEquals(0,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));
    Assertions.assertEquals(0,
        pendingOps.getPendingOps(new ContainerID(1)).size());
    Assertions.assertEquals(0,
        pendingOps.getPendingOps(new ContainerID(2)).size());

  }

  @Test
  public void testCanAddReplicasForAdd() {
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn1, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn2, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn3, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    // Duplicate for DN2
    pendingOps.scheduleAddReplica(
        ContainerID.valueOf(1), dn2, 0, addCmd, deadline + 1,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    // Not a duplicate for DN2 as different index. Should not happen in practice as it is not valid to have 2 indexes
    // on the same node.
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn2, 1, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleAddReplica(ContainerID.valueOf(2), dn1, 1, addCmd, deadline,
        THREE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleAddReplica(
        ContainerID.valueOf(2), dn1, 1, addCmd, deadline + 1,
        THREE_GB_CONTAINER_SIZE, clock.millis());

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(4, ops.size());
    for (ContainerReplicaOp op : ops) {
      if (!op.getTarget().equals(dn2)) {
        assertEquals(0, op.getReplicaIndex());
      }
      assertEquals(ADD, op.getOpType());
      if (op.getTarget().equals(dn2) && op.getReplicaIndex() == 0) {
        assertEquals(deadline + 1, op.getDeadlineEpochMillis());
      } else {
        assertEquals(deadline, op.getDeadlineEpochMillis());
      }
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(ContainerReplicaOp::getTarget).collect(Collectors.toList());
    Assertions.assertTrue(allDns.contains(dn1));
    Assertions.assertTrue(allDns.contains(dn2));
    Assertions.assertTrue(allDns.contains(dn3));

    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertEquals(1, ops.size());
    assertEquals(1, ops.get(0).getReplicaIndex());
    assertEquals(ADD, ops.get(0).getOpType());
    assertEquals(dn1, ops.get(0).getTarget());
    assertEquals(deadline + 1, ops.get(0).getDeadlineEpochMillis());
  }

  @Test
  public void testCanAddReplicasForDelete() {
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deleteCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn3, 0, deleteCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deleteCmd, deadline);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(3, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assertions.assertEquals(0, op.getReplicaIndex());
      Assertions.assertEquals(DELETE, op.getOpType());
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(ContainerReplicaOp::getTarget).collect(Collectors.toList());
    Assertions.assertTrue(allDns.contains(dn1));
    Assertions.assertTrue(allDns.contains(dn2));
    Assertions.assertTrue(allDns.contains(dn3));

    ops = pendingOps.getPendingOps(new ContainerID(2));
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(DELETE, ops.get(0).getOpType());
    Assertions.assertEquals(dn1, ops.get(0).getTarget());
  }

  @Test
  public void testCompletingOps() {
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(1), dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn1, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(1), dn2, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn3, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(2), dn1, 1, deleteCmd, deadline);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));

    // We expect 4 entries - 2 add and 2 delete.
    Assertions.assertEquals(4, ops.size());

    Assertions.assertTrue(pendingOps
        .completeAddReplica(new ContainerID(1), dn1, 0));
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(3, ops.size());

    // Complete one that does not exist:
    Assertions.assertFalse(pendingOps
        .completeAddReplica(new ContainerID(1), dn1, 0));
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(3, ops.size());

    // Complete the remaining ones
    pendingOps.completeDeleteReplica(new ContainerID(1), dn1, 0);
    pendingOps.completeDeleteReplica(new ContainerID(1), dn2, 0);
    pendingOps.completeAddReplica(new ContainerID(1), dn3, 0);
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(0, ops.size());
  }

  @Test
  public void testRemoveSpecificOp() {
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(1), dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn1, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(1), dn2, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn3, 0, addCmd, deadline,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(2), dn1, 1, deleteCmd, deadline);

    ContainerID cid = new ContainerID(1);
    List<ContainerReplicaOp> ops = pendingOps.getPendingOps(cid);
    Assertions.assertEquals(4, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assertions.assertTrue(pendingOps.removeOp(cid, op));
    }
    // Attempt to remove one that no longer exists
    Assertions.assertFalse(pendingOps.removeOp(cid, ops.get(0)));
    ops = pendingOps.getPendingOps(cid);
    Assertions.assertEquals(0, ops.size());
  }

  @Test
  public void testRemoveExpiredEntries() {
    long expiry = clock.millis() + 1000;
    long laterExpiry =  clock.millis() + 2000;
    long latestExpiry = clock.millis() + 3000;
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(1), dn1, 0, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn1, 0, addCmd, expiry,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(1), dn2, 0, deleteCmd, laterExpiry);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn3, 0, addCmd, laterExpiry,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(2), dn1, 1, deleteCmd, latestExpiry);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(2), dn1, 1, addCmd, latestExpiry,
        FIVE_GB_CONTAINER_SIZE, clock.millis());

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(4, ops.size());
    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertEquals(2, ops.size());

    // Some entries expire at "start + 1000" some at start + 2000 and
    // start + 3000. Clock is currently at "start"
    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // Nothing is remove as no deadline is older than the current clock time.
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(4, ops.size());

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // Those ADD with deadline + 1000 should be removed, but deletes are retained
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(3, ops.size());
    // We should lose the entries for DN1
    assertFalse(isOpPresent(ops, dn1, 0, ADD));
    assertTrue(isOpPresent(ops, dn1, 0, DELETE));
    assertTrue(isOpPresent(ops, dn2, 0, DELETE));

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();

    // Now should only have entries for container 2 and the deletes for container 1
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(2, ops.size());

    assertTrue(isOpPresent(ops, dn1, 0, DELETE));
    assertTrue(isOpPresent(ops, dn2, 0, DELETE));

    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertEquals(2, ops.size());

    // Advance the clock again and all should be removed except deletes
    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertTrue(isOpPresent(ops, dn1, 1, DELETE));
    assertEquals(1, ops.size());
  }

  private boolean isOpPresent(List<ContainerReplicaOp> ops, DatanodeDetails dn,
      int index, ContainerReplicaOp.PendingOpType type) {
    return ops.stream().anyMatch(op -> op.getTarget().equals(dn) &&
        op.getReplicaIndex() == index && op.getOpType() == type);
  }

  @Test
  public void testReplicationMetrics() {
    long expiry = clock.millis() + 1000;
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(1), dn1, 1, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn1, 2, addCmd, expiry,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(2), dn2, 1, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(2), dn3, 1, addCmd, expiry,
        THREE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleAddReplica(ContainerID.valueOf(3), dn3, 0, addCmd, expiry,
        THREE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(4), dn3, 0, deleteCmd, expiry);

    // InFlight Replication and Deletion
    Assertions.assertEquals(3, pendingOps.getPendingOpCount(ADD));
    Assertions.assertEquals(3, pendingOps.getPendingOpCount(DELETE));
    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(ADD, ReplicationType.RATIS));
    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(DELETE, ReplicationType.RATIS));
    Assertions.assertEquals(2,
        pendingOps.getPendingOpCount(ADD, ReplicationType.EC));
    Assertions.assertEquals(2,
        pendingOps.getPendingOpCount(DELETE, ReplicationType.EC));

    clock.fastForward(1500);

    pendingOps.removeExpiredEntries();

    // Two Delete and Replication command should be timeout
    Assertions.assertEquals(metrics.getEcReplicaCreateTimeoutTotal(), 2);
    Assertions.assertEquals(metrics.getEcReplicaDeleteTimeoutTotal(), 2);
    Assertions.assertEquals(metrics.getReplicaCreateTimeoutTotal(), 1);
    Assertions.assertEquals(metrics.getReplicaDeleteTimeoutTotal(), 1);

    expiry = clock.millis() + 1000;
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(3), dn1, 2, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(3), dn1, 3, addCmd, expiry,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(4), dn2, 2, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(ContainerID.valueOf(4), dn3, 4, addCmd, expiry,
        THREE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleAddReplica(ContainerID.valueOf(5), dn3, 0, addCmd, expiry,
        THREE_GB_CONTAINER_SIZE, clock.millis());
    pendingOps.scheduleDeleteReplica(ContainerID.valueOf(6), dn3, 0, deleteCmd, expiry);

    // InFlight Replication and Deletion. Previous Inflight should be
    // removed as they were timed out, but deletes are retained
    assertEquals(3, pendingOps.getPendingOpCount(ADD));
    assertEquals(6, pendingOps.getPendingOpCount(DELETE));

    pendingOps.completeDeleteReplica(new ContainerID(3), dn1, 2);
    pendingOps.completeAddReplica(new ContainerID(3), dn1, 3);
    pendingOps.completeDeleteReplica(new ContainerID(4), dn2, 2);
    pendingOps.completeAddReplica(new ContainerID(4), dn3, 4);
    pendingOps.completeDeleteReplica(new ContainerID(6), dn3, 0);
    pendingOps.completeAddReplica(new ContainerID(5), dn3, 0);

    Assertions.assertEquals(metrics.getEcReplicasCreatedTotal(), 2);
    Assertions.assertEquals(metrics.getEcReplicasDeletedTotal(), 2);
    Assertions.assertEquals(metrics.getReplicasCreatedTotal(), 1);
    Assertions.assertEquals(metrics.getReplicasDeletedTotal(), 1);

    pendingOps.completeDeleteReplica(new ContainerID(3), dn1, 2);
    pendingOps.completeAddReplica(new ContainerID(2), dn1, 3);

    // Checking pendingOpCount doesn't go below zero
    assertEquals(0, pendingOps.getPendingOpCount(ADD));
    assertEquals(3, pendingOps.getPendingOpCount(DELETE));
  }

  /**
   * Tests that registered subscribers are notified about completed and expired
   * ops.
   */
  @Test
  public void testNotifySubscribers() {
    // register subscribers
    ContainerReplicaPendingOpsSubscriber subscriber1 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    ContainerReplicaPendingOpsSubscriber subscriber2 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);
    pendingOps.registerSubscriber(subscriber2);

    // schedule an ADD and a DELETE
    ContainerID containerID = ContainerID.valueOf(1);
    pendingOps.scheduleAddReplica(containerID, dn1, 0, addCmd, deadline, FIVE_GB_CONTAINER_SIZE, clock.millis());
    ContainerReplicaOp addOp = pendingOps.getPendingOps(containerID).get(0);
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deleteCmd, deadline);

    // complete the ADD and verify that subscribers were notified
    pendingOps.completeAddReplica(containerID, dn1, 0);
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(addOp,
        containerID, false);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(addOp,
        containerID, false);

    // complete the DELETE and verify subscribers were notified
    ContainerReplicaOp deleteOp = pendingOps.getPendingOps(containerID).get(0);
    pendingOps.completeDeleteReplica(containerID, dn1, 0);
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(deleteOp,
        containerID, false);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(deleteOp,
        containerID, false);

    // now, test notification on expiration
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline, FIVE_GB_CONTAINER_SIZE, clock.millis());
    for (ContainerReplicaOp op : pendingOps.getPendingOps(containerID)) {
      if (op.getOpType() == ADD) {
        addOp = op;
      } else {
        deleteOp = op;
      }
    }
    clock.fastForward(20000);
    pendingOps.removeExpiredEntries();
    // the clock is at 1000 and commands expired at 500
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(addOp,
        containerID, true);
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(deleteOp,
        containerID, true);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(addOp,
        containerID, true);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(deleteOp,
        containerID, true);
  }

  @Test
  public void subscribersShouldNotBeNotifiedWhenOpsHaveNotExpired() {
    ContainerID containerID = new ContainerID(1);

    // schedule ops
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline, FIVE_GB_CONTAINER_SIZE, clock.millis());

    // register subscriber
    ContainerReplicaPendingOpsSubscriber subscriber1 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // no entries have expired, so there should be zero interactions with the
    // subscriber
    verifyNoMoreInteractions(subscriber1);
  }

  @Test
  public void subscribersShouldNotBeNotifiedWhenReplacingAnOpWithDuplicate() {
    ContainerID containerID = new ContainerID(1);

    // schedule ops
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline, FIVE_GB_CONTAINER_SIZE, clock.millis());

    // register subscriber
    ContainerReplicaPendingOpsSubscriber subscriber1 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);

    clock.fastForward(1000);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline + 1,
        FIVE_GB_CONTAINER_SIZE, clock.millis());
    // no entries have expired, so there should be zero interactions with the
    // subscriber
    verifyNoMoreInteractions(subscriber1);
  }

  /**
   * Tests that ContainerReplicaPendingOps correctly tracks how much size (of containers) is being moved to a target
   * Datanode because of pending ADD ops. This size should be correctly added and reduced when ADD ops are triggered
   * and completed.
   */
  @Test
  public void testScheduledSizeIsCorrectlyTrackedAndCompleted() {
    final long eventTimeout = rmConf.getEventTimeout();
    long now = clock.millis();
    pendingOps.scheduleAddReplica(ContainerID.valueOf(1), dn1, 0, addCmd,
        now + eventTimeout, FIVE_GB_CONTAINER_SIZE, clock.millis());

    // Assert that containerSizeScheduled has the correct size
    ConcurrentHashMap<UUID, ContainerReplicaPendingOps.SizeAndTime> scheduled =
        pendingOps.getContainerSizeScheduled();
    assertEquals(1, scheduled.size());
    assertEquals(FIVE_GB_CONTAINER_SIZE, scheduled.get(dn1.getUuid()).getSize());

    // Schedule a second op for the same datanode
    pendingOps.scheduleAddReplica(ContainerID.valueOf(2), dn1, 0, addCmd,
        now + eventTimeout, THREE_GB_CONTAINER_SIZE, clock.millis());
    assertEquals(FIVE_GB_CONTAINER_SIZE + THREE_GB_CONTAINER_SIZE, scheduled.get(dn1.getUuid()).getSize());

    // Complete the first op
    pendingOps.completeAddReplica(ContainerID.valueOf(1), dn1, 0);
    assertEquals(THREE_GB_CONTAINER_SIZE, scheduled.get(dn1.getUuid()).getSize());

    // Complete the second op
    pendingOps.completeAddReplica(ContainerID.valueOf(2), dn1, 0);
    Assertions.assertNull(scheduled.get(dn1.getUuid()));
  }

  /**
   * When an ADD op (container replication) expires, the map in ContainerReplicaPendingOps should be modified
   * correctly. The entry should be removed if ReplicationManagerConfiguration#eventTimemout milliseconds have passed
   * since the entry's lastUpdatedTime.
   */
  @Test
  public void testScheduledSizeIsCorrectlyTrackedAndExpired() {
    final long eventTimeout = rmConf.getEventTimeout();

    long now = clock.millis();
    pendingOps.scheduleAddReplica(ContainerID.valueOf(3), dn2, 0, addCmd,
        now + eventTimeout, FIVE_GB_CONTAINER_SIZE, clock.millis());
    ConcurrentHashMap<UUID, ContainerReplicaPendingOps.SizeAndTime>
        scheduled = pendingOps.getContainerSizeScheduled();
    assertEquals(FIVE_GB_CONTAINER_SIZE, scheduled.get(dn2.getUuid()).getSize());
    assertEquals(now, scheduled.get(dn2.getUuid()).getLastUpdatedTime());

    // Advance clock so the op expires
    clock.fastForward(eventTimeout + 1);
    pendingOps.removeExpiredEntries();
    // The entry should be removed from the map after expiration
    Assertions.assertNull(scheduled.get(dn2.getUuid()));
  }

  /**
   * Tests that only the size of containers with expired ops is reduced from the map tracking size of pending ops.
   * For example, if target Datanode DN1 has two pending ADD ops 10GB + 15GB, and the first op expires, then only
   * 10GB should be subtracted.
   */
  @Test
  public void testOnlyExpiredOpSizeIsRemovedFromSizeScheduledMap() {
    final long eventTimeout = rmConf.getEventTimeout();
    long now = clock.millis();
    // Schedule first op
    pendingOps.scheduleAddReplica(ContainerID.valueOf(4), dn2, 0, addCmd,
        now + eventTimeout, FIVE_GB_CONTAINER_SIZE, clock.millis());
    //  another replication scheduled for dn1 to receive a container - just testing that this entry isn't removed or
    //  modified when other entries expire or are modified
    pendingOps.scheduleAddReplica((ContainerID.valueOf(2)), dn1, 2, addCmd,
        now + eventTimeout * 10, THREE_GB_CONTAINER_SIZE, clock.millis());
    ConcurrentHashMap<UUID, ContainerReplicaPendingOps.SizeAndTime>
        scheduled = pendingOps.getContainerSizeScheduled();
    assertEquals(FIVE_GB_CONTAINER_SIZE, scheduled.get(dn2.getUuid()).getSize());
    assertEquals(THREE_GB_CONTAINER_SIZE, scheduled.get(dn1.getUuid()).getSize());
    assertEquals(now, scheduled.get(dn2.getUuid()).getLastUpdatedTime());
    assertEquals(now, scheduled.get(dn1.getUuid()).getLastUpdatedTime());

    clock.fastForward(eventTimeout - 1);
    long updateTime = clock.millis();

    // Schedule second op for dn2, which should update the lastUpdatedTime
    pendingOps.scheduleAddReplica(ContainerID.valueOf(5), dn2, 1, addCmd,
        updateTime + eventTimeout, THREE_GB_CONTAINER_SIZE, clock.millis());
    assertEquals(FIVE_GB_CONTAINER_SIZE + THREE_GB_CONTAINER_SIZE, scheduled.get(dn2.getUuid()).getSize());
    assertEquals(THREE_GB_CONTAINER_SIZE, scheduled.get(dn1.getUuid()).getSize());
    assertEquals(updateTime, scheduled.get(dn2.getUuid()).getLastUpdatedTime());
    assertEquals(now, scheduled.get(dn1.getUuid()).getLastUpdatedTime());

    // Advance clock to expire the first op but not the second for dn2
    clock.set(Instant.ofEpochMilli(now + eventTimeout + 1));
    pendingOps.removeExpiredEntries();

    // Assert the entry for dn2 still exists, but with reduced size
    Assertions.assertNotNull(scheduled.get(dn2.getUuid()));
    assertEquals(THREE_GB_CONTAINER_SIZE, scheduled.get(dn2.getUuid()).getSize());
    assertEquals(THREE_GB_CONTAINER_SIZE, scheduled.get(dn1.getUuid()).getSize());

    // Advance clock again to expire the second op for dn2
    clock.set(Instant.ofEpochMilli(updateTime + eventTimeout + 1));
    pendingOps.removeExpiredEntries();
    Assertions.assertNull(scheduled.get(dn2.getUuid()));
    assertEquals(THREE_GB_CONTAINER_SIZE, scheduled.get(dn1.getUuid()).getSize());
  }
}
