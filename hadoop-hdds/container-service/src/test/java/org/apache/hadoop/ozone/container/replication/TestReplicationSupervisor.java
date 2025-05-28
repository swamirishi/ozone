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

package org.apache.hadoop.ozone.container.replication;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.ArrayList;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.common.volume.VolumeInfo;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCommandInfo;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status.DONE;
import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.fromSources;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.LOW;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.NORMAL;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the replication supervisor.
 */
@RunWith(Parameterized.class)
public class TestReplicationSupervisor {

  private static final long CURRENT_TERM = 1;

  @TempDir
  private File tempDir;

  private final ContainerReplicator noopReplicator = task -> { };
  private final ContainerReplicator throwingReplicator = task -> {
    throw new RuntimeException("testing replication failure");
  };
  private final ContainerReplicator slowReplicator = task -> {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
  };
  private final AtomicReference<ContainerReplicator> replicatorRef =
      new AtomicReference<>();

  private ContainerSet set;

  private ContainerLayoutVersion layoutVersion;

  private final ContainerLayoutVersion layout;

  private StateContext context;
  private TestClock clock;
  private DatanodeDetails datanode;
  private VolumeChoosingPolicy volumeChoosingPolicy;

  public TestReplicationSupervisor(ContainerLayoutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  @Before
  public void setUp() throws Exception {
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    set = new ContainerSet(1000);
    DatanodeStateMachine stateMachine =
        mock(DatanodeStateMachine.class);
    context = new StateContext(
        new OzoneConfiguration(),
        DatanodeStateMachine.DatanodeStates.getInitState(),
        stateMachine);
    context.setTermOfLeaderSCM(CURRENT_TERM);
    datanode = MockDatanodeDetails.randomDatanodeDetails();
    when(stateMachine.getDatanodeDetails()).thenReturn(datanode);
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(new OzoneConfiguration());
  }

  @After
  public void cleanup() {
    replicatorRef.set(null);
  }

  @Test
  public void normal() {
    // GIVEN
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);
    ReplicationSupervisorMetrics metrics =
        ReplicationSupervisorMetrics.create(supervisor);

    try {
      //WHEN
      supervisor.addTask(createTask(1L));
      supervisor.addTask(createTask(2L));
      supervisor.addTask(createTask(5L));

      Assert.assertEquals(3, supervisor.getReplicationRequestCount());
      Assert.assertEquals(3, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(3, set.containerCount());

      MetricsCollectorImpl metricsCollector = new MetricsCollectorImpl();
      metrics.getMetrics(metricsCollector, true);
      Assert.assertEquals(1, metricsCollector.getRecords().size());
    } finally {
      metrics.unRegister();
      supervisor.stop();
    }
  }

  @Test
  public void duplicateMessage() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWithReplicator(
        FakeReplicator::new);

    try {
      //WHEN
      supervisor.addTask(createTask(6L));
      supervisor.addTask(createTask(6L));
      supervisor.addTask(createTask(6L));
      supervisor.addTask(createTask(6L));

      //THEN
      Assert.assertEquals(4, supervisor.getReplicationRequestCount());
      Assert.assertEquals(1, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(3, supervisor.getReplicationSkippedCount());
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(1, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void failureHandling() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(
        __ -> throwingReplicator, newDirectExecutorService());

    try {
      //WHEN
      ReplicationTask task = createTask(1L);
      supervisor.addTask(task);

      //THEN
      Assert.assertEquals(1, supervisor.getReplicationRequestCount());
      Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(1, supervisor.getReplicationFailureCount());
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(0, set.containerCount());
      Assert.assertEquals(ReplicationTask.Status.FAILED, task.getStatus());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void stalledDownload() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(__ -> noopReplicator,
        new DiscardingExecutorService());

    try {
      //WHEN
      supervisor.addTask(createTask(1L));
      supervisor.addTask(createTask(2L));
      supervisor.addTask(createTask(3L));
      supervisor.addTask(createECTask(4L));
      supervisor.addTask(createECTask(5L));

      //THEN
      Assert.assertEquals(0, supervisor.getReplicationRequestCount());
      Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(5, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(3, supervisor.getInFlightReplications(
          ReplicationTask.class));
      Assert.assertEquals(2, supervisor.getInFlightReplications(
          ECReconstructionCoordinatorTask.class));
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(0, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void slowDownload() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(__ -> slowReplicator,
        new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()));

    try {
      //WHEN
      supervisor.addTask(createTask(1L));
      supervisor.addTask(createTask(2L));
      supervisor.addTask(createTask(3L));

      //THEN
      Assert.assertEquals(3, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(2, supervisor.getQueueSize());
      // Sleep 4s, wait all tasks processed
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
      }
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void testDownloadAndImportReplicatorFailure() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();

    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .executor(newDirectExecutorService())
        .clock(clock)
        .build();

    // Mock to fetch an exception in the importContainer method.
    SimpleContainerDownloader moc =
        mock(SimpleContainerDownloader.class);
    Path res = Paths.get("file:/tmp/no-such-file");
    when(
        moc.getContainerDataFromReplicas(anyLong(), anyList(),
            any(Path.class), any()))
        .thenReturn(res);

    final String testDir = GenericTestUtils.getTempPath(
        TestReplicationSupervisor.class.getSimpleName() +
            "-" + UUID.randomUUID());
    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    when(volumeSet.getVolumesList())
        .thenReturn(singletonList(
            new HddsVolume.Builder(testDir).conf(conf).build()));
    ContainerImporter importer =
        new ContainerImporter(conf, set, null, volumeSet, volumeChoosingPolicy);
    ContainerReplicator replicator =
        new DownloadAndImportReplicator(conf, set, importer, moc);

    replicatorRef.set(replicator);

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(DownloadAndImportReplicator.LOG);

    supervisor.addTask(createTask(1L));
    Assert.assertEquals(1, supervisor.getReplicationFailureCount());
    Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
    Assert.assertTrue(logCapturer.getOutput()
        .contains("Container 1 replication was unsuccessful."));
  }

  @Test
  public void testReplicationImportReserveSpace()
      throws IOException, InterruptedException, TimeoutException {
    final long containerUsedSize = 100;
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, tempDir.getAbsolutePath());

    long containerMaxSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .executor(newDirectExecutorService())
        .clock(clock)
        .build();

    MutableVolumeSet volumeSet = new MutableVolumeSet(datanode.getUuidString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);

    long containerId = 1;
    // create container
    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, containerMaxSize, "test", "test");
    HddsVolume vol1 = (HddsVolume) volumeSet.getVolumesList().get(0);
    containerData.setVolume(vol1);
    containerData.incrBytesUsed(containerUsedSize);
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    ContainerController controllerMock = mock(ContainerController.class);
    Semaphore semaphore = new Semaphore(1);
    when(controllerMock.importContainer(any(), any(), any()))
        .thenAnswer((invocation) -> {
          semaphore.acquire();
          return container;
        });

    File tarFile = containerTarFile(containerId, containerData);

    SimpleContainerDownloader moc =
        mock(SimpleContainerDownloader.class);
    when(
        moc.getContainerDataFromReplicas(anyLong(), anyList(),
            any(Path.class), any()))
        .thenReturn(tarFile.toPath());

    ContainerImporter importer =
        new ContainerImporter(conf, set, controllerMock, volumeSet, volumeChoosingPolicy);

    // Initially volume has 0 commit space
    assertEquals(0, vol1.getCommittedBytes());
    long usedSpace = vol1.getUsedSpace();
    // Initially volume has 0 used space
    assertEquals(0, usedSpace);
    // Increase committed bytes so that volume has only remaining 3 times container size space
    long minFreeSpace =
        conf.getObject(DatanodeConfiguration.class).getMinFreeSpace(vol1.getVolumeInfo().get().getCurrentUsage().getCapacity());
    long initialCommittedBytes =
        vol1.getVolumeInfo().get().getCurrentUsage().getCapacity() - containerMaxSize * 3 - minFreeSpace;
    vol1.incCommittedBytes(initialCommittedBytes);
    ContainerReplicator replicator =
        new DownloadAndImportReplicator(conf, set, importer, moc);
    replicatorRef.set(replicator);

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(DownloadAndImportReplicator.LOG);

    // Acquire semaphore so that container import will pause after reserving space.
    semaphore.acquire();
    CompletableFuture.runAsync(() -> {
      try {
        supervisor.addTask(createTask(containerId));
      } catch (Exception ex) {
      }
    });

    // Wait such that first container import reserve space
    GenericTestUtils.waitFor(() ->
        vol1.getCommittedBytes() > initialCommittedBytes,
        1000, 50000);

    // Volume has reserved space of 2 * containerSize
    assertEquals(vol1.getCommittedBytes(), initialCommittedBytes + 2 * containerMaxSize);
    // Container 2 import will fail as container 1 has reserved space and no space left to import new container
    // New container import requires at least (2 * container size)
    long containerId2 = 2;
    supervisor.addTask(createTask(containerId2));
    GenericTestUtils.waitFor(() -> 1 == supervisor.getReplicationFailureCount(),
        1000, 50000);
    assertThat(logCapturer.getOutput()).contains("No volumes have enough space for a new container");
    // Release semaphore so that first container import will pass
    semaphore.release();
    GenericTestUtils.waitFor(() ->
        1 == supervisor.getReplicationSuccessCount(), 1000, 50000);

    usedSpace = vol1.getUsedSpace();
    // After replication, volume used space should be increased by container used bytes
    assertEquals(containerUsedSize, usedSpace);

    // Volume committed bytes used for replication has been released, no need to reserve space for imported container
    // only closed container gets replicated, so no new data will be written it
    assertEquals(vol1.getCommittedBytes(), initialCommittedBytes);

  }


  private File containerTarFile(
      long containerId, ContainerData containerData) throws IOException {
    File yamlFile = new File(tempDir, "container.yaml");
    ContainerDataYaml.createContainerFile(
            ContainerProtos.ContainerType.KeyValueContainer, containerData,
            yamlFile);
    File tarFile = new File(tempDir,
        ContainerUtils.getContainerTarName(containerId));
    try (OutputStream output = Files.newOutputStream(tarFile.toPath())) {
      ArchiveOutputStream<TarArchiveEntry> archive = new TarArchiveOutputStream(output);
      TarArchiveEntry entry = archive.createArchiveEntry(yamlFile,
          "container.yaml");
      archive.putArchiveEntry(entry);
      try (InputStream input = Files.newInputStream(yamlFile.toPath())) {
        IOUtils.copy(input, archive);
      }
      archive.closeArchiveEntry();
    }
    return tarFile;
  }

  @Test
  public void testTaskBeyondDeadline() {
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);

    ReplicateContainerCommand cmd = createCommand(1);
    cmd.setDeadline(clock.millis() + 10000);
    ReplicationTask task1 = new ReplicationTask(cmd, replicatorRef.get());
    cmd = createCommand(2);
    cmd.setDeadline(clock.millis() + 20000);
    ReplicationTask task2 = new ReplicationTask(cmd, replicatorRef.get());
    cmd = createCommand(3);
    // No deadline set
    ReplicationTask task3 = new ReplicationTask(cmd, replicatorRef.get());
    // no deadline set

    clock.fastForward(15000);

    supervisor.addTask(task1);
    supervisor.addTask(task2);
    supervisor.addTask(task3);

    Assert.assertEquals(3, supervisor.getReplicationRequestCount());
    Assert.assertEquals(2, supervisor.getReplicationSuccessCount());
    Assert.assertEquals(0, supervisor.getReplicationFailureCount());
    Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
    Assert.assertEquals(0, supervisor.getQueueSize());
    Assert.assertEquals(1, supervisor.getReplicationTimeoutCount());
    Assert.assertEquals(2, set.containerCount());

  }

  @Test
  public void testDatanodeOutOfService() {
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);
    datanode.setPersistedOpState(
        HddsProtos.NodeOperationalState.DECOMMISSIONING);

    ReplicateContainerCommand pushCmd = ReplicateContainerCommand.toTarget(
        1, MockDatanodeDetails.randomDatanodeDetails());
    pushCmd.setTerm(CURRENT_TERM);
    ReplicateContainerCommand pullCmd = createCommand(2);

    supervisor.addTask(new ReplicationTask(pushCmd, replicatorRef.get()));
    supervisor.addTask(new ReplicationTask(pullCmd, replicatorRef.get()));

    Assert.assertEquals(2, supervisor.getReplicationRequestCount());
    Assert.assertEquals(1, supervisor.getReplicationSuccessCount());
    Assert.assertEquals(0, supervisor.getReplicationFailureCount());
    Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
    Assert.assertEquals(0, supervisor.getQueueSize());
    Assert.assertEquals(0, supervisor.getReplicationTimeoutCount());
    Assert.assertEquals(1, set.containerCount());
  }

  @Test
  public void taskWithObsoleteTermIsDropped() {
    final long newTerm = 2;
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);

    context.setTermOfLeaderSCM(newTerm);
    supervisor.addTask(createTask(1L));

    Assert.assertEquals(1, supervisor.getReplicationRequestCount());
    Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
  }

  @Test
  public void testPriorityOrdering() throws InterruptedException {
    long deadline = clock.millis() + 1000;
    long containerId = 1;
    long term = 1;
    OzoneConfiguration conf = new OzoneConfiguration();
    ReplicationServer.ReplicationConfig repConf =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    repConf.setReplicationMaxStreams(1);
    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .replicationConfig(repConf)
        .clock(clock)
        .build();

    final CountDownLatch indicateRunning = new CountDownLatch(1);
    final CountDownLatch completeRunning = new CountDownLatch(1);
    // Going to create 5 tasks below, so this counter needs to be set to 5.
    final CountDownLatch tasksCompleteLatch = new CountDownLatch(5);

    supervisor.addTask(new BlockingTask(containerId, deadline, term,
        indicateRunning, completeRunning));
    // Wait for the first task to block the single threaded executor
    indicateRunning.await();

    List<String> completionOrder = new ArrayList<>();
    // Now load some tasks out of order.
    clock.fastForward(10);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        LOW, "LOW_10", completionOrder, tasksCompleteLatch));
    clock.rewind(5);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        LOW, "LOW_5", completionOrder, tasksCompleteLatch));

    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        NORMAL, "HIGH_5", completionOrder, tasksCompleteLatch));
    clock.rewind(4);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        NORMAL, "HIGH_1", completionOrder, tasksCompleteLatch));
    clock.fastForward(10);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        NORMAL, "HIGH_11", completionOrder, tasksCompleteLatch));

    List<String> expectedOrder = new ArrayList<>();
    expectedOrder.add("HIGH_1");
    expectedOrder.add("HIGH_5");
    expectedOrder.add("HIGH_11");
    expectedOrder.add("LOW_5");
    expectedOrder.add("LOW_10");

    // Before unblocking the queue, check the queue count for the OrderedTask.
    // We loaded 3 High / normal priority and 2 low. The counter should not
    // include the low counts.
    Assert.assertEquals(3,
        supervisor.getInFlightReplications(OrderedTask.class));
    Assert.assertEquals(1,
        supervisor.getInFlightReplications(BlockingTask.class));

    // Unblock the queue
    completeRunning.countDown();
    // Wait for all tasks to complete
    tasksCompleteLatch.await();
    Assert.assertEquals(expectedOrder, completionOrder);
    Assert.assertEquals(0,
        supervisor.getInFlightReplications(OrderedTask.class));
    Assert.assertEquals(0,
        supervisor.getInFlightReplications(BlockingTask.class));
  }

  private static class BlockingTask extends AbstractReplicationTask {

    private final CountDownLatch runningLatch;
    private final CountDownLatch waitForCompleteLatch;

    BlockingTask(long containerId, long deadlineEpochMs, long term,
        CountDownLatch running, CountDownLatch waitForCompletion) {
      super(containerId, deadlineEpochMs, term);
      this.runningLatch = running;
      this.waitForCompleteLatch = waitForCompletion;
    }

    @Override
    public void runTask() {
      runningLatch.countDown();
      try {
        waitForCompleteLatch.await();
      } catch (InterruptedException e) {
        fail("Interrupted waiting for the completion latch to be released");
      }
      setStatus(DONE);
    }
  }

  private static class OrderedTask extends  AbstractReplicationTask {

    private final String name;
    private final List<String> completeList;
    private final CountDownLatch completeLatch;

    @SuppressWarnings("checkstyle:parameterNumber")
    OrderedTask(long containerId, long deadlineEpochMs, long term,
        Clock clock, ReplicationCommandPriority priority,
        String name, List<String> completeList, CountDownLatch completeLatch) {
      super(containerId, deadlineEpochMs, term, clock);
      this.completeList = completeList;
      this.name = name;
      this.completeLatch = completeLatch;
      setPriority(priority);
    }

    @Override
    public void runTask() {
      completeList.add(name);
      setStatus(DONE);
      completeLatch.countDown();
    }
  }

  private ReplicationSupervisor supervisorWithReplicator(
      Function<ReplicationSupervisor, ContainerReplicator> replicatorFactory) {
    return supervisorWith(replicatorFactory, newDirectExecutorService());
  }

  private ReplicationSupervisor supervisorWith(
      Function<ReplicationSupervisor, ContainerReplicator> replicatorFactory,
      ExecutorService executor) {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationServer.ReplicationConfig repConf =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .replicationConfig(repConf)
        .executor(executor)
        .clock(clock)
        .build();
    replicatorRef.set(replicatorFactory.apply(supervisor));
    return supervisor;
  }

  private ReplicationTask createTask(long containerId) {
    ReplicateContainerCommand cmd = createCommand(containerId);
    return new ReplicationTask(cmd, replicatorRef.get());
  }

  private ECReconstructionCoordinatorTask createECTask(long containerId) {
    return new ECReconstructionCoordinatorTask(null,
        createReconstructionCmd(containerId));
  }

  private static ReplicateContainerCommand createCommand(long containerId) {
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.forTest(containerId);
    cmd.setTerm(CURRENT_TERM);
    return cmd;
  }

  private static ECReconstructionCommandInfo createReconstructionCmd(
      long containerId) {
    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex> sources
        = new ArrayList<>();
    sources.add(new ReconstructECContainersCommand
        .DatanodeDetailsAndReplicaIndex(
            MockDatanodeDetails.randomDatanodeDetails(), 1));
    sources.add(new ReconstructECContainersCommand
        .DatanodeDetailsAndReplicaIndex(
        MockDatanodeDetails.randomDatanodeDetails(), 2));
    sources.add(new ReconstructECContainersCommand
        .DatanodeDetailsAndReplicaIndex(
        MockDatanodeDetails.randomDatanodeDetails(), 3));

    byte[] missingIndexes = new byte[1];
    missingIndexes[0] = 4;

    List<DatanodeDetails> target = singletonList(
        MockDatanodeDetails.randomDatanodeDetails());
    ReconstructECContainersCommand cmd =
        new ReconstructECContainersCommand(containerId,
            sources,
            target,
            missingIndexes,
            new ECReplicationConfig(3, 2));

    return new ECReconstructionCommandInfo(cmd);
  }

  /**
   * A fake replicator that simulates successful download of containers.
   */
  private class FakeReplicator implements ContainerReplicator {

    private final OzoneConfiguration conf = new OzoneConfiguration();
    private final ReplicationSupervisor supervisor;

    FakeReplicator(ReplicationSupervisor supervisor) {
      this.supervisor = supervisor;
    }

    @Override
    public void replicate(ReplicationTask task) {
      if (set.getContainer(task.getContainerId()) != null) {
        task.setStatus(AbstractReplicationTask.Status.SKIPPED);
        return;
      }

      // assumes same-thread execution
      Assert.assertEquals(1, supervisor.getTotalInFlightReplications());

      KeyValueContainerData kvcd =
          new KeyValueContainerData(task.getContainerId(),
              layout, 100L,
              UUID.randomUUID().toString(), UUID.randomUUID().toString());
      KeyValueContainer kvc =
          new KeyValueContainer(kvcd, conf);

      try {
        set.addContainer(kvc);
        task.setStatus(DONE);
      } catch (Exception e) {
        Assert.fail("Unexpected error: " + e.getMessage());
      }
    }
  }

  /**
   * Discards all tasks.
   */
  private static class DiscardingExecutorService
      extends AbstractExecutorService {

    @Override
    public void shutdown() {
      // no-op
    }

    @Override
    public @Nonnull List<Runnable> shutdownNow() {
      return emptyList();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
      return false;
    }

    @Override
    public void execute(@Nonnull Runnable command) {
      // ignore all tasks
    }
  }

  @Test
  public void poolSizeCanBeIncreased() {
    datanode.setPersistedOpState(IN_SERVICE);
    ReplicationSupervisor subject = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .build();

    try {
      subject.nodeStateUpdated(ENTERING_MAINTENANCE);
    } finally {
      subject.stop();
    }
  }

  @Test
  public void poolSizeCanBeDecreased() {
    datanode.setPersistedOpState(IN_MAINTENANCE);
    ReplicationSupervisor subject = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .build();

    try {
      subject.nodeStateUpdated(IN_SERVICE);
    } finally {
      subject.stop();
    }
  }

  @Test
  public void testMaxQueueSize() {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());

    final int maxQueueSize = 2;
    DatanodeConfiguration datanodeConfig = new DatanodeConfiguration();
    datanodeConfig.setCommandQueueLimit(maxQueueSize);

    final int replicationMaxStreams = 5;
    ReplicationServer.ReplicationConfig repConf =
        new ReplicationServer.ReplicationConfig();
    repConf.setReplicationMaxStreams(replicationMaxStreams);

    AtomicInteger threadPoolSize = new AtomicInteger();

    ReplicationSupervisor rs = ReplicationSupervisor.newBuilder()
        .executor(new DiscardingExecutorService())
        .executorThreadUpdater(threadPoolSize::set)
        .datanodeConfig(datanodeConfig)
        .replicationConfig(repConf)
        .build();

    scheduleTasks(datanodes, rs);

    // in progress task will be limited by max. queue size,
    // since all tasks are discarded by the executor, none of them complete
    Assert.assertEquals(maxQueueSize, rs.getTotalInFlightReplications());

    // queue size is doubled
    rs.nodeStateUpdated(HddsProtos.NodeOperationalState.DECOMMISSIONING);
    Assert.assertEquals(2 * maxQueueSize, rs.getMaxQueueSize());
    Assert.assertEquals(2 * replicationMaxStreams, threadPoolSize.get());

    // can schedule more tasks
    scheduleTasks(datanodes, rs);
    Assert.assertEquals(2 * maxQueueSize, rs.getTotalInFlightReplications());

    // queue size is restored
    rs.nodeStateUpdated(IN_SERVICE);
    Assert.assertEquals(maxQueueSize, rs.getMaxQueueSize());
    Assert.assertEquals(replicationMaxStreams, threadPoolSize.get());
  }

  //schedule 10 container replication
  private void scheduleTasks(
      List<DatanodeDetails> datanodes, ReplicationSupervisor rs) {
    for (int i = 0; i < 10; i++) {
      List<DatanodeDetails> sources =
          singletonList(datanodes.get(i % datanodes.size()));
      rs.addTask(new ReplicationTask(fromSources(i, sources), noopReplicator));
    }
  }
}
