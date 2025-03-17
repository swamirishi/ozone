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

package org.apache.hadoop.ozone.recon.tasks.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to iterate through a table in parallel by breaking table into multiple iterators.
 */
public class ParallelTableIteratorOperation<K extends Comparable<K>, V> implements Closeable {
  private final Table<K, V> table;
  private final Codec<K> keyCodec;
  private final ThreadPoolExecutor iteratorExecutor;
  private final OMMetadataManager metadataManager;
  private final int maxIteratorTasks;
  private final long logCountThreshold;

  private static final Logger LOG = LoggerFactory.getLogger(ParallelTableIteratorOperation.class);
  public ParallelTableIteratorOperation(OMMetadataManager metadataManager, Table<K, V> table, Codec<K> keyCodec,
                                        int iteratorCount, long logThreshold) {
    this.table = table;
    this.keyCodec = keyCodec;
    this.metadataManager = metadataManager;
    this.maxIteratorTasks = 2 * iteratorCount;
    this.iteratorExecutor = new ThreadPoolExecutor(iteratorCount, iteratorCount, 1, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>());
    this.logCountThreshold = logThreshold;
  }


  private List<K> getBounds(K startKey, K endKey) throws IOException {
    RDBStore store = (RDBStore) this.metadataManager.getStore();
    List<LiveFileMetaData> sstFiles = store.getDb().getSstFileList();
    Set<K> keys = new HashSet<>();
    for (LiveFileMetaData sstFile : sstFiles) {
      if (StringCodec.get().fromPersistedFormat(sstFile.columnFamilyName()).equals(table.getName())) {
        keys.add(this.keyCodec.fromPersistedFormat(sstFile.smallestKey()));
        keys.add(this.keyCodec.fromPersistedFormat(sstFile.largestKey()));
      }
    }
    List<K> boundKeys = new ArrayList<>();
    boundKeys.add(startKey);
    boundKeys.addAll(keys.stream().sorted().filter(Objects::nonNull)
            .filter(key -> startKey == null || key.compareTo(startKey) >= 0)
            .filter(key -> endKey == null || endKey.compareTo(key) >= 0)
            .collect(Collectors.toList()));
    boundKeys.add(endKey);
    return boundKeys;
  }

  private void waitForQueueSize(Queue<Future<?>> futures, int expectedSize)
          throws ExecutionException, InterruptedException {
    while (!futures.isEmpty() && futures.size() > expectedSize) {
      Future<?> f = futures.poll();
      if (f != null) {
        f.get();
      }
    }
  }

  public void performTaskOnTableVals(String taskName, K startKey, K endKey,
      Function<Table.KeyValue<K, V>, Void> keyOperation) throws IOException, ExecutionException, InterruptedException {
    List<K> bounds = null;
    try {
      bounds = getBounds(startKey, endKey);
    } catch (IOException e) {
      LOG.warn("Error while getting bounds for task: {}. Table", taskName, e);
      bounds = Arrays.asList(startKey, endKey);
    }
    Queue<Future<?>> iterFutures = new LinkedList<>();
    AtomicLong keyCounter = new AtomicLong();
    AtomicLong prevLogCounter = new AtomicLong();
    final String tableName = table.getName();
    for (int idx = 1; idx < bounds.size(); idx++) {
      K beg = bounds.get(idx - 1);
      K end = bounds.get(idx);
      boolean inclusive = idx == bounds.size() - 1;
      waitForQueueSize(iterFutures, maxIteratorTasks - 1);
      iterFutures.add(iteratorExecutor.submit(() -> {
        try (TableIterator<K, ? extends Table.KeyValue<K, V>> iter  = table.iterator()) {
          if (beg != null) {
            iter.seek(beg);
          } else {
            iter.seekToFirst();
          }
          while (iter.hasNext()) {
            Table.KeyValue<K, V> kv = iter.next();
            if (end == null || kv.getKey().compareTo(end) < 0 || (inclusive && kv.getKey().compareTo(endKey) <= 0)) {
              keyOperation.apply(kv);
              keyCounter.incrementAndGet();
              if (keyCounter.get() - prevLogCounter.get() > logCountThreshold) {
                LOG.info("Iterated through table : {} {} keys while performing task: {}", tableName,
                  keyCounter.get(), taskName);
                prevLogCounter.set(keyCounter.get());
              }
            } else {
              break;
            }
          }
        } catch (IOException e) {
          LOG.error("Error while performing task", e);
          throw new RuntimeException(e);
        }
      }));
    }
    waitForQueueSize(iterFutures, 0);
  }

  @Override
  public void close() throws IOException {
    iteratorExecutor.shutdown();
  }
}
