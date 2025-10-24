/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Class used to test ContainerSet operations.
 */
@RunWith(Parameterized.class)
public class TestContainerSet {

  private static final int FIRST_ID = 2;

  private final ContainerLayoutVersion layout;

  public TestContainerSet(ContainerLayoutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  /**
   * Create a mock {@link HddsVolume} to track container IDs.
   */
  private HddsVolume mockHddsVolume(String storageId) {
    HddsVolume volume = mock(HddsVolume.class);
    when(volume.getStorageID()).thenReturn(storageId);

    ConcurrentSkipListSet<Long> containerIds = new ConcurrentSkipListSet<>();

    doAnswer(inv -> {
      Long containerId = inv.getArgument(0);
      containerIds.add(containerId);
      return null;
    }).when(volume).addContainer(any(Long.class));

    doAnswer(inv -> {
      Long containerId = inv.getArgument(0);
      containerIds.remove(containerId);
      return null;
    }).when(volume).removeContainer(any(Long.class));

    when(volume.getContainerIterator()).thenAnswer(inv -> containerIds.iterator());
    when(volume.getContainerCount()).thenAnswer(inv -> (long) containerIds.size());

    return volume;
  }

  @Test
  public void testAddGetRemoveContainer() throws StorageContainerException {
    ContainerSet containerSet = new ContainerSet(1000);
    long containerId = 100L;
    ContainerProtos.ContainerDataProto.State state = ContainerProtos
        .ContainerDataProto.State.CLOSED;

    KeyValueContainerData kvData = new KeyValueContainerData(containerId,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    kvData.setState(state);
    KeyValueContainer keyValueContainer = new KeyValueContainer(kvData, new
        OzoneConfiguration());

    //addContainer
    boolean result = containerSet.addContainer(keyValueContainer);
    assertTrue(result);
    try {
      containerSet.addContainer(keyValueContainer);
      fail("Adding same container ID twice should fail.");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Container already exists with" +
          " container Id " + containerId, ex);
    }

    //getContainer
    KeyValueContainer container = (KeyValueContainer) containerSet
        .getContainer(containerId);
    KeyValueContainerData keyValueContainerData =
        container.getContainerData();
    assertEquals(containerId, keyValueContainerData.getContainerID());
    assertEquals(state, keyValueContainerData.getState());
    assertNull(containerSet.getContainer(1000L));

    //removeContainer
    assertTrue(containerSet.removeContainer(containerId));
    assertFalse(containerSet.removeContainer(1000L));
  }

  @Test
  public void testIteratorsAndCount() throws StorageContainerException {

    ContainerSet containerSet = createContainerSet();

    assertEquals(10, containerSet.containerCount());

    int count = 0;
    for (Container<?> kv : containerSet) {
      ContainerData containerData = kv.getContainerData();
      long containerId = containerData.getContainerID();
      if (containerId % 2 == 0) {
        assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
            containerData.getState());
      } else {
        assertEquals(ContainerProtos.ContainerDataProto.State.OPEN,
            containerData.getState());
      }
      count++;
    }
    assertEquals(10, count);

    //Using containerMapIterator.
    Iterator<Map.Entry<Long, Container<?>>> containerMapIterator = containerSet
        .getContainerMapIterator();

    count = 0;
    while (containerMapIterator.hasNext()) {
      Container kv = containerMapIterator.next().getValue();
      ContainerData containerData = kv.getContainerData();
      long containerId = containerData.getContainerID();
      if (containerId % 2 == 0) {
        assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
            containerData.getState());
      } else {
        assertEquals(ContainerProtos.ContainerDataProto.State.OPEN,
            containerData.getState());
      }
      count++;
    }
    assertEquals(10, count);

  }

  @Test
  public void testIteratorPerVolume() throws StorageContainerException {
    HddsVolume vol1 = mockHddsVolume("uuid-1");
    HddsVolume vol2 = mockHddsVolume("uuid-2");

    ContainerSet containerSet = new ContainerSet(1000);
    for (int i = 0; i < 10; i++) {
      KeyValueContainerData kvData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      if (i % 2 == 0) {
        kvData.setVolume(vol1);
      } else {
        kvData.setVolume(vol2);
      }
      kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
      KeyValueContainer kv = new KeyValueContainer(kvData, new
              OzoneConfiguration());
      containerSet.addContainer(kv);
    }

    Iterator<Container<?>> iter1 = containerSet.getContainerIterator(vol1);
    int count1 = 0;
    while (iter1.hasNext()) {
      Container c = iter1.next();
      assertEquals(0, (c.getContainerData().getContainerID() % 2));
      count1++;
    }
    assertEquals(5, count1);

    Iterator<Container<?>> iter2 = containerSet.getContainerIterator(vol2);
    int count2 = 0;
    while (iter2.hasNext()) {
      Container c = iter2.next();
      assertEquals(1, (c.getContainerData().getContainerID() % 2));
      count2++;
    }
    assertEquals(5, count2);
  }

  @Test
  public void iteratorIsOrderedByScanTime() throws StorageContainerException {
    HddsVolume vol = mockHddsVolume("uuid-1");
    Random random = new Random();
    ContainerSet containerSet = new ContainerSet(1000);
    int containerCount = 50;
    for (int i = 0; i < containerCount; i++) {
      KeyValueContainerData kvData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      if (random.nextBoolean()) {
        Instant scanTime = Instant.ofEpochMilli(Math.abs(random.nextLong()));
        kvData.updateDataScanTime(scanTime);
      }
      kvData.setVolume(vol);
      kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
      KeyValueContainer kv = new KeyValueContainer(kvData, new
          OzoneConfiguration());
      containerSet.addContainer(kv);
    }

    int containersToBeScanned = 0;
    Optional<Instant> prevScanTime = Optional.empty();
    long prevContainerID = Long.MIN_VALUE;
    for (Iterator<Container<?>> iter = containerSet.getContainerIterator(vol);
         iter.hasNext();) {
      ContainerData data = iter.next().getContainerData();
      Optional<Instant> scanTime = data.lastDataScanTime();
      if (prevScanTime.isPresent()) {
        if (scanTime.isPresent()) {
          int result = scanTime.get().compareTo(prevScanTime.get());
          assertTrue(result >= 0);
          if (result == 0) {
            assertTrue(prevContainerID < data.getContainerID());
          }
        } else {
          fail("Containers not yet scanned should be sorted before " +
              "already scanned ones");
        }
      }

      prevScanTime = scanTime;
      prevContainerID = data.getContainerID();
      containersToBeScanned++;
    }

    assertEquals(containerCount, containersToBeScanned);
  }

  @Test
  public void testGetContainerReport() throws IOException {

    ContainerSet containerSet = createContainerSet();

    ContainerReportsProto containerReportsRequestProto = containerSet
        .getContainerReport();

    assertEquals(10, containerReportsRequestProto.getReportsList().size());
  }



  @Test
  public void testListContainer() throws StorageContainerException {
    ContainerSet containerSet = createContainerSet();
    int count = 5;
    int startId = FIRST_ID + 3;
    List<ContainerData> result = new ArrayList<>(count);

    containerSet.listContainer(startId, count, result);

    assertContainerIds(startId, count, result);
  }

  @Test
  public void testListContainerFromFirstKey() throws StorageContainerException {
    ContainerSet containerSet = createContainerSet();
    int count = 6;
    List<ContainerData> result = new ArrayList<>(count);

    containerSet.listContainer(0, count, result);

    assertContainerIds(FIRST_ID, count, result);
  }

  /**
   * Verify that {@code result} contains {@code count} containers
   * with IDs in increasing order starting at {@code startId}.
   */
  private static void assertContainerIds(int startId, int count,
                                         List<ContainerData> result) {
    assertEquals(count, result.size());
    assertArrayEquals(LongStream.range(startId, startId + count).toArray(),
        result.stream().mapToLong(ContainerData::getContainerID).toArray());
  }

  private ContainerSet createContainerSet() throws StorageContainerException {
    ContainerSet containerSet = new ContainerSet(1000);
    for (int i = FIRST_ID; i < FIRST_ID + 10; i++) {
      KeyValueContainerData kvData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      if (i % 2 == 0) {
        kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
      } else {
        kvData.setState(ContainerProtos.ContainerDataProto.State.OPEN);
      }
      KeyValueContainer kv = new KeyValueContainer(kvData, new
          OzoneConfiguration());
      containerSet.addContainer(kv);
    }
    return containerSet;
  }

  /**
   * Test that containerCount per volume returns correct count.
   */
  @Test
  public void testContainerCountPerVolume() throws StorageContainerException {
    HddsVolume vol1 = mockHddsVolume("uuid-1");
    HddsVolume vol2 = mockHddsVolume("uuid-2");
    HddsVolume vol3 = mockHddsVolume("uuid-3");

    ContainerSet containerSet = new ContainerSet(1000);

    // Add 100 containers to vol1, 50 to vol2, 0 to vol3
    for (int i = 0; i < 100; i++) {
      KeyValueContainerData kvData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      kvData.setVolume(vol1);
      kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
      containerSet.addContainer(new KeyValueContainer(kvData, new OzoneConfiguration()));
    }

    for (int i = 100; i < 150; i++) {
      KeyValueContainerData kvData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      kvData.setVolume(vol2);
      kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
      containerSet.addContainer(new KeyValueContainer(kvData, new OzoneConfiguration()));
    }

    // Verify counts
    assertEquals(100, containerSet.containerCount(vol1));
    assertEquals(50, containerSet.containerCount(vol2));
    assertEquals(0, containerSet.containerCount(vol3));

    // Remove some containers and verify counts are updated
    containerSet.removeContainer(0);
    containerSet.removeContainer(1);
    containerSet.removeContainer(100);
    assertEquals(98, containerSet.containerCount(vol1));
    assertEquals(49, containerSet.containerCount(vol2));
  }

  /**
   * Test that per-volume iterator only returns containers from that volume.
   */
  @Test
  public void testContainerIteratorPerVolume() throws StorageContainerException {
    HddsVolume vol1 = mockHddsVolume("uuid-11");
    HddsVolume vol2 = mockHddsVolume("uuid-12");

    ContainerSet containerSet = new ContainerSet(1000);

    // Add containers with specific IDs to each volume
    List<Long> vol1Ids = new ArrayList<>();
    List<Long> vol2Ids = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      KeyValueContainerData kvData = new KeyValueContainerData(i,
          layout,
          (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      if (i % 2 == 0) {
        kvData.setVolume(vol1);
        vol1Ids.add((long) i);
      } else {
        kvData.setVolume(vol2);
        vol2Ids.add((long) i);
      }
      kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);
      containerSet.addContainer(new KeyValueContainer(kvData, new OzoneConfiguration()));
    }

    // Verify iterator only returns containers from vol1
    Iterator<Container<?>> iter1 = containerSet.getContainerIterator(vol1);
    List<Long> foundVol1Ids = new ArrayList<>();
    while (iter1.hasNext()) {
      foundVol1Ids.add(iter1.next().getContainerData().getContainerID());
    }
    assertEquals(vol1Ids.size(), foundVol1Ids.size());
    assertTrue(foundVol1Ids.containsAll(vol1Ids));

    // Verify iterator only returns containers from vol2
    Iterator<Container<?>> iter2 = containerSet.getContainerIterator(vol2);
    List<Long> foundVol2Ids = new ArrayList<>();
    while (iter2.hasNext()) {
      foundVol2Ids.add(iter2.next().getContainerData().getContainerID());
    }
    assertEquals(vol2Ids.size(), foundVol2Ids.size());
    assertTrue(foundVol2Ids.containsAll(vol2Ids));
  }

}
