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

package org.apache.hadoop.ozone.om.lock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock.Resource;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock.FlatResource;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Class tests OzoneManagerLock.
 */
public class TestOzoneManagerLock {

  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneManagerLock.class);

  @Test
  public void acquireResourceLock() {
    String[] resourceName;
    for (LeveledResource resource : LeveledResource.values()) {
      resourceName = generateResourceName(resource);
      testResourceLock(resourceName, resource);
    }
  }

  private void testResourceLock(String[] resourceName, LeveledResource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(resource, resourceName);
    lock.releaseWriteLock(resource, resourceName);
    assertTrue(true);
  }

  @Test
  public void reacquireResourceLock() {
    String[] resourceName;
    for (LeveledResource resource : LeveledResource.values()) {
      resourceName = generateResourceName(resource);
      testResourceReacquireLock(resourceName, resource);
    }
  }

  private void testResourceReacquireLock(String[] resourceName,
                                         LeveledResource resource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    // Lock re-acquire not allowed by same thread.
    if (resource == LeveledResource.USER_LOCK ||
        resource == LeveledResource.S3_SECRET_LOCK ||
        resource == LeveledResource.PREFIX_LOCK) {
      lock.acquireWriteLock(resource, resourceName);
      try {
        lock.acquireWriteLock(resource, resourceName);
        fail("reacquireResourceLock failed");
      } catch (RuntimeException ex) {
        String message = "cannot acquire " + resource.getName() + " lock " +
            "while holding [" + resource.getName() + "] lock(s).";
        assertTrue(ex.getMessage(), ex.getMessage().contains(message));
      }
      lock.releaseWriteLock(resource, resourceName);
      assertTrue(true);
    } else {
      lock.acquireWriteLock(resource, resourceName);
      lock.acquireWriteLock(resource, resourceName);
      lock.releaseWriteLock(resource, resourceName);
      lock.releaseWriteLock(resource, resourceName);
      assertTrue(true);
    }
  }

  @Test
  public void testLockingOrder() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire lock, and then in inner loop acquire all locks with higher
    // lock level, finally release the locks.
    for (LeveledResource resource : LeveledResource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      resourceName = generateResourceName(resource);
      lock.acquireWriteLock(resource, resourceName);
      stack.push(new ResourceInfo(resourceName, resource));
      for (LeveledResource higherResource : LeveledResource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
        }
      }
      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseWriteLock(resourceInfo.getResource(),
            resourceInfo.getLockName());
      }
    }
    assertTrue(true);
  }

  @ParameterizedTest
  @EnumSource
  public void testFlatLockWithParallelResource(FlatResource flatResource) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    List<Resource> resources = new ArrayList<>();
    resources.addAll(Arrays.stream(LeveledResource.values()).collect(Collectors.toList()));
    resources.addAll(Arrays.stream(FlatResource.values()).collect(Collectors.toList()));
    for (Resource otherResource : resources) {
      String[] otherResourceName = generateResourceName(otherResource);
      String[] flatResourceName = generateResourceName(flatResource);
      lock.acquireWriteLock(otherResource, otherResourceName);
      try {
        lock.acquireWriteLock(flatResource, flatResourceName);
      } finally {
        lock.releaseWriteLock(otherResource, otherResourceName);
        lock.releaseWriteLock(flatResource, flatResourceName);
      }
    }
  }

  @Test
  public void testLockViolationsWithOneHigherLevelLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    for (LeveledResource resource : LeveledResource.values()) {
      for (LeveledResource higherResource : LeveledResource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          String[] resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          try {
            lock.acquireWriteLock(resource, generateResourceName(resource));
            fail("testLockViolationsWithOneHigherLevelLock failed");
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding [" + higherResource.getName() + "] lock(s).";
            assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
          lock.releaseWriteLock(higherResource, resourceName);
        }
      }
    }
  }

  @Test
  public void testLockViolations() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String[] resourceName;

    // What this test does is iterate all resources. For each resource
    // acquire an higher level lock above the resource, and then take the the
    // lock. This should fail. Like that it tries all error combinations.
    for (LeveledResource resource : LeveledResource.values()) {
      Stack<ResourceInfo> stack = new Stack<>();
      List<String> currentLocks = new ArrayList<>();
      for (LeveledResource higherResource : LeveledResource.values()) {
        if (higherResource.getMask() > resource.getMask()) {
          resourceName = generateResourceName(higherResource);
          lock.acquireWriteLock(higherResource, resourceName);
          stack.push(new ResourceInfo(resourceName, higherResource));
          currentLocks.add(higherResource.getName());
          // try to acquire lower level lock
          try {
            resourceName = generateResourceName(resource);
            lock.acquireWriteLock(resource, resourceName);
          } catch (RuntimeException ex) {
            String message = "cannot acquire " + resource.getName() + " lock " +
                "while holding " + currentLocks.toString() + " lock(s).";
            assertTrue(ex.getMessage(),
                ex.getMessage().contains(message));
          }
        }
      }

      // Now release locks
      while (!stack.empty()) {
        ResourceInfo resourceInfo = stack.pop();
        lock.releaseWriteLock(resourceInfo.getResource(),
            resourceInfo.getLockName());
      }
    }
  }

  @Test
  public void releaseLockWithOutAcquiringLock() {
    OzoneManagerLock lock =
        new OzoneManagerLock(new OzoneConfiguration());
    assertThrows(IllegalMonitorStateException.class,
        () -> lock.releaseWriteLock(LeveledResource.USER_LOCK, "user3"));
  }

  private String[] generateResourceName(Resource resource) {
    if (resource == LeveledResource.BUCKET_LOCK) {
      return new String[]{UUID.randomUUID().toString(),
          UUID.randomUUID().toString()};
    } else if ((resource == LeveledResource.KEY_PATH_LOCK) ||
        (resource == LeveledResource.SNAPSHOT_LOCK)) {
      return new String[]{UUID.randomUUID().toString(),
          UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    } else {
      return new String[]{UUID.randomUUID().toString()};
    }
  }

  /**
   * Class used to store locked resource info.
   */
  public static class ResourceInfo {
    private String[] lockName;
    private LeveledResource resource;

    ResourceInfo(String[] resourceName, LeveledResource resource) {
      this.lockName = resourceName;
      this.resource = resource;
    }

    public String[] getLockName() {
      return lockName.clone();
    }

    public LeveledResource getResource() {
      return resource;
    }
  }

  @Test
  public void acquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    lock.releaseMultiUserLock("user1", "user2");
    assertTrue(true);
  }

  @Test
  public void reAcquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    try {
      lock.acquireMultiUserLock("user1", "user2");
      fail("reAcquireMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock("user1", "user2");
  }

  @Test
  public void acquireMultiUserLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireWriteLock(LeveledResource.USER_LOCK, "user3");
    try {
      lock.acquireMultiUserLock("user1", "user2");
      fail("acquireMultiUserLockAfterUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseWriteLock(LeveledResource.USER_LOCK, "user3");
  }

  @Test
  public void acquireUserLockAfterMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user1", "user2");
    try {
      lock.acquireWriteLock(LeveledResource.USER_LOCK, "user3");
      fail("acquireUserLockAfterMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER_LOCK lock while holding " +
          "[USER_LOCK] lock(s).";
      assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock("user1", "user2");
  }

  @Test
  public void testLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (LeveledResource resource :
        LeveledResource.values()) {
      final String[] resourceName = generateResourceName(resource);
      lock.acquireWriteLock(resource, resourceName);

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        lock.acquireWriteLock(resource, resourceName);
        gotLock.set(true);
        lock.releaseWriteLock(resource, resourceName);
      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same resource,
      // it will wait.
      assertFalse(gotLock.get());
      lock.releaseWriteLock(resource, resourceName);
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      assertTrue(gotLock.get());
    }

  }

  @Test
  public void testMultiLocksResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    for (LeveledResource resource : LeveledResource.values()) {
      final List<String[]> resourceName = Arrays.asList(generateResourceName(resource),
          generateResourceName(resource), generateResourceName(resource));
      lock.acquireWriteLocks(resource, resourceName.subList(1, resourceName.size()));

      AtomicBoolean gotLock = new AtomicBoolean(false);
      new Thread(() -> {
        lock.acquireWriteLocks(resource, resourceName.subList(0, 2));
        gotLock.set(true);
        lock.releaseWriteLocks(resource, resourceName.subList(0, 2));
      }).start();
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      // Since the new thread is trying to get lock on same resource,
      // it will wait.
      assertFalse(gotLock.get());
      lock.releaseWriteLocks(resource, resourceName.subList(1, resourceName.size()));
      // Since we have released the lock, the new thread should have the lock
      // now.
      // Let's give some time for the new thread to run
      Thread.sleep(100);
      assertTrue(gotLock.get());
    }

  }


  @Test
  public void testMultiLockResourceParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("user2", "user1");

    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireMultiUserLock("user1", "user2");
      gotLock.set(true);
      lock.releaseMultiUserLock("user1", "user2");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same resource, it will
    // wait.
    assertFalse(gotLock.get());
    lock.releaseMultiUserLock("user2", "user1");
    // Since we have released the lock, the new thread should have the lock
    // now.
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    assertTrue(gotLock.get());
  }

  @Test
  public void testLockHoldCount() {
    String[] resourceName;
    for (LeveledResource resource : LeveledResource.values()) {
      // USER_LOCK, S3_SECRET_LOCK and PREFIX_LOCK disallow lock re-acquire by
      // the same thread.
      if (resource != LeveledResource.USER_LOCK &&
          resource != LeveledResource.S3_SECRET_LOCK &&
          resource != LeveledResource.PREFIX_LOCK) {
        resourceName = generateResourceName(resource);
        testLockHoldCountUtil(resource, resourceName);
      }
    }
  }

  private void testLockHoldCountUtil(LeveledResource resource,
                                     String[] resourceName) {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());

    assertEquals(0, lock.getReadHoldCount(resource, resourceName));
    lock.acquireReadLock(resource, resourceName);
    assertEquals(1, lock.getReadHoldCount(resource, resourceName));

    lock.acquireReadLock(resource, resourceName);
    assertEquals(2, lock.getReadHoldCount(resource, resourceName));

    lock.releaseReadLock(resource, resourceName);
    assertEquals(1, lock.getReadHoldCount(resource, resourceName));

    lock.releaseReadLock(resource, resourceName);
    assertEquals(0, lock.getReadHoldCount(resource, resourceName));

    assertFalse(
        lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(0, lock.getWriteHoldCount(resource, resourceName));
    lock.acquireWriteLock(resource, resourceName);
    assertTrue(
        lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(1, lock.getWriteHoldCount(resource, resourceName));

    lock.acquireWriteLock(resource, resourceName);
    assertTrue(
        lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(2, lock.getWriteHoldCount(resource, resourceName));

    lock.releaseWriteLock(resource, resourceName);
    assertTrue(
        lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(1, lock.getWriteHoldCount(resource, resourceName));

    lock.releaseWriteLock(resource, resourceName);
    assertFalse(
        lock.isWriteLockedByCurrentThread(resource, resourceName));
    assertEquals(0, lock.getWriteHoldCount(resource, resourceName));
  }

  @Test
  public void testLockConcurrentStats() throws InterruptedException {
    String[] resourceName;
    for (LeveledResource resource : LeveledResource.values()) {
      resourceName = generateResourceName(resource);
      testReadLockConcurrentStats(resource, resourceName, 10);
      testWriteLockConcurrentStats(resource, resourceName, 5);
      testSyntheticReadWriteLockConcurrentStats(resource, resourceName, 10, 3);
    }
  }

  public void testReadLockConcurrentStats(LeveledResource resource,
      String[] resourceName, int threadCount) throws InterruptedException {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        lock.acquireReadLock(resource, resourceName);
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseReadLock(resource, resourceName);
      });
      threads[i].start();
    }

    for (Thread t : threads) {
      t.join();
    }

    String readHeldStat = lock.getOMLockMetrics().getReadLockHeldTimeMsStat();
    assertTrue(
        "Expected " + threadCount +
            " samples in readLockHeldTimeMsStat: " + readHeldStat,
        readHeldStat.contains("Samples = " + threadCount));

    String readWaitingStat =
        lock.getOMLockMetrics().getReadLockWaitingTimeMsStat();
    assertTrue(
        "Expected " + threadCount +
            " samples in readLockWaitingTimeMsStat: " + readWaitingStat,
        readWaitingStat.contains("Samples = " + threadCount));
  }

  public void testWriteLockConcurrentStats(LeveledResource resource,
      String[] resourceName, int threadCount) throws InterruptedException {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        lock.acquireWriteLock(resource, resourceName);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseWriteLock(resource, resourceName);
      });
      threads[i].start();
    }

    for (Thread t : threads) {
      t.join();
    }

    String writeHeldStat = lock.getOMLockMetrics().getWriteLockHeldTimeMsStat();
    assertTrue(
        "Expected " + threadCount +
            " samples in writeLockHeldTimeMsStat: " + writeHeldStat,
        writeHeldStat.contains("Samples = " + threadCount));

    String writeWaitingStat =
        lock.getOMLockMetrics().getWriteLockWaitingTimeMsStat();
    assertTrue(
        "Expected " + threadCount +
            " samples in writeLockWaitingTimeMsStat" + writeWaitingStat,
        writeWaitingStat.contains("Samples = " + threadCount));
  }

  public void testSyntheticReadWriteLockConcurrentStats(
      LeveledResource resource, String[] resourceName,
      int readThreadCount, int writeThreadCount)
      throws InterruptedException {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    Thread[] readThreads = new Thread[readThreadCount];
    Thread[] writeThreads = new Thread[writeThreadCount];

    for (int i = 0; i < readThreads.length; i++) {
      readThreads[i] = new Thread(() -> {
        lock.acquireReadLock(resource, resourceName);
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseReadLock(resource, resourceName);
      });
      readThreads[i].setName("ReadLockThread-" + i);
      readThreads[i].start();
    }

    for (int i = 0; i < writeThreads.length; i++) {
      writeThreads[i] = new Thread(() -> {
        lock.acquireWriteLock(resource, resourceName);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        lock.releaseWriteLock(resource, resourceName);
      });
      writeThreads[i].setName("WriteLockThread-" + i);
      writeThreads[i].start();
    }

    for (Thread r : readThreads) {
      r.join();
    }

    for (Thread w : writeThreads) {
      w.join();
    }

    String readHeldStat = lock.getOMLockMetrics().getReadLockHeldTimeMsStat();
    assertTrue(
        "Expected " + readThreadCount +
            " samples in readLockHeldTimeMsStat: " + readHeldStat,
        readHeldStat.contains("Samples = " + readThreadCount));

    String readWaitingStat =
        lock.getOMLockMetrics().getReadLockWaitingTimeMsStat();
    assertTrue(
        "Expected " + readThreadCount +
            " samples in readLockWaitingTimeMsStat: " + readWaitingStat,
        readWaitingStat.contains("Samples = " + readThreadCount));

    String writeHeldStat = lock.getOMLockMetrics().getWriteLockHeldTimeMsStat();
    assertTrue(
        "Expected " + writeThreadCount +
            " samples in writeLockHeldTimeMsStat: " + writeHeldStat,
        writeHeldStat.contains("Samples = " + writeThreadCount));

    String writeWaitingStat =
        lock.getOMLockMetrics().getWriteLockWaitingTimeMsStat();
    assertTrue(
        "Expected " + writeThreadCount +
            " samples in writeLockWaitingTimeMsStat" + writeWaitingStat,
        writeWaitingStat.contains("Samples = " + writeThreadCount));
  }

  @Test
  public void testOMLockMetricsRecords() {
    OMLockMetrics omLockMetrics = OMLockMetrics.create();
    try {
      MetricsCollectorImpl metricsCollector = new MetricsCollectorImpl();
      omLockMetrics.getMetrics(metricsCollector, true);
      Assert.assertEquals(1, metricsCollector.getRecords().size());

      String omLockMetricsRecords = metricsCollector.getRecords().toString();
      assertTrue(omLockMetricsRecords,
          omLockMetricsRecords.contains("ReadLockWaitingTime"));
      assertTrue(omLockMetricsRecords,
          omLockMetricsRecords.contains("ReadLockHeldTime"));
      assertTrue(omLockMetricsRecords,
          omLockMetricsRecords.contains("WriteLockWaitingTime"));
      assertTrue(omLockMetricsRecords,
          omLockMetricsRecords.contains("WriteLockHeldTime"));
    } finally {
      omLockMetrics.unRegister();
    }
  }
}
