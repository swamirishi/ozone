/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;

/**
 * Class to iterate through a table in parallel by breaking table into multiple iterators for RDB store.
 */
public class ParallelTableIterOperator<RAW, K, V> {
  private final Supplier<KeyValueIterator<RAW, RAW>> threadSafeIteratorSupplier;
  private final String tableName;
  private final ThrottledThreadpoolExecutor executor;
  private final Comparator<K> comparator;
  private final CheckedFunction<RAW, K, Throwable> keyConverter;
  private final CheckedFunction<RAW, V, Throwable> valueConverter;
  private final Lock lock;

  public ParallelTableIterOperator(String tableName,
                                   ThrottledThreadpoolExecutor throttledThreadpoolExecutor,
                                   Supplier<KeyValueIterator<RAW, RAW>> threadUnsafeIteratorSupplier,
                                   CheckedFunction<RAW, K, Throwable> keyConverter,
                                   CheckedFunction<RAW, V, Throwable> valueConverter,
                                   Comparator<K> comparator) {
    this.tableName = tableName;
    this.executor = throttledThreadpoolExecutor;
    this.threadSafeIteratorSupplier = threadUnsafeIteratorSupplier;
    this.comparator = comparator;
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;
    this.lock = new ReentrantLock();
  }

  @SuppressWarnings("parameternumber")
  private <THROWABLE extends Throwable> CompletableFuture<Void> submit(
      CheckedFunction<Table.KeyValue<K, V>, Void, THROWABLE> keyOperation,
      K end, AtomicLong keyCounter, AtomicLong prevLogCounter, long logCountThreshold, Logger log,
      AtomicBoolean cancelled) throws InterruptedException {
    return executor.submit(() -> {
      try (KeyValueIterator<RAW, RAW> iter = threadSafeIteratorSupplier.get()) {
        while (iter.hasNext() && !cancelled.get()) {
          lock.lock();
          Table.KeyValue<RAW, RAW> rawKV;
          try {
            if (!iter.hasNext()) {
              break;
            }
            rawKV = iter.next();
          } finally {
            lock.unlock();
          }
          Table.KeyValue<K, V> kv = Table.newKeyValue(keyConverter.apply(rawKV.getKey()),
              valueConverter.apply(rawKV.getValue()));
          if (end == null || Objects.compare(kv.getKey(), end, comparator) < 0) {
            keyOperation.apply(kv);
            keyCounter.incrementAndGet();
            if (keyCounter.get() - prevLogCounter.get() > logCountThreshold) {
              log.info("Iterated through table : {} {} keys while performing task.", tableName,
                  keyCounter.get());
              prevLogCounter.set(keyCounter.get());
            }
          } else {
            break;
          }
        }
      }
    });
  }

  public  <THROWABLE extends Throwable> void performTaskOnTableVals(
      K endKey, CheckedFunction<Table.KeyValue<K, V>, Void, THROWABLE> keyOperation,
      Logger log, long logCountThreshold)
      throws ExecutionException, InterruptedException {
    AtomicLong keyCounter = new AtomicLong();
    AtomicLong prevLogCounter = new AtomicLong();
    CompletableFuture<Void> iterFutures = CompletableFuture.completedFuture(null);
    AtomicBoolean cancelled = new AtomicBoolean(false);
    for (int idx = 0; idx < executor.getMaxNumberOfThreads(); idx++) {
      if (cancelled.get()) {
        break;
      }
      CompletableFuture<Void> future = submit(keyOperation, endKey, keyCounter, prevLogCounter,
          logCountThreshold, log, cancelled);
      future.exceptionally((e -> {
        cancelled.set(true);
        return null;
      }));
      iterFutures = iterFutures.thenCombine(future, (v1, v2) -> null);
    }
    iterFutures.get();
  }
}
