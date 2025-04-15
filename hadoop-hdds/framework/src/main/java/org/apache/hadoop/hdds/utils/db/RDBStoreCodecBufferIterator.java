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

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.commons.lang3.exception.UncheckedInterruptedException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;

/**
 * Implement {@link RDBStoreAbstractIterator} using {@link CodecBuffer}.
 * Any Key or Value returned will be only valid within the lifecycle of this iterator.
 */
class RDBStoreCodecBufferIterator
    extends RDBStoreAbstractIterator<CodecBuffer> {
  static class Buffer {
    private final CodecBuffer.Capacity initialCapacity;
    private final PutToByteBuffer<RuntimeException> source;
    private CodecBuffer buffer;

    Buffer(CodecBuffer.Capacity initialCapacity,
        PutToByteBuffer<RuntimeException> source) {
      this.initialCapacity = initialCapacity;
      this.source = source;
    }

    void release() {
      if (buffer != null) {
        buffer.release();
      }
    }

    private void prepare() {
      if (buffer == null) {
        allocate();
      } else {
        buffer.clear();
      }
    }

    private void allocate() {
      if (buffer != null) {
        buffer.release();
      }
      buffer = CodecBuffer.allocateDirect(-initialCapacity.get());
    }

    CodecBuffer getFromDb() {
      for (prepare(); ; allocate()) {
        final Integer required = buffer.putFromSource(source);
        if (required == null) {
          return null; // the source is unavailable
        } else if (required == buffer.readableBytes()) {
          return buffer; // buffer size is big enough
        }
        // buffer size too small, try increasing the capacity.
        if (buffer.setCapacity(required)) {
          buffer.clear();
          // retry with the new capacity
          final int retried = buffer.putFromSource(source);
          Preconditions.assertSame(required.intValue(), retried, "required");
          return buffer;
        }

        // failed to increase the capacity
        // increase initial capacity and reallocate it
        initialCapacity.increase(required);
      }
    }
  }

  private final String name;
  private final Set<RawKeyValue<Buffer>> inUseBuffers;
  private final AtomicBoolean closed = new AtomicBoolean();

  RDBStoreCodecBufferIterator(ManagedRocksIterator iterator, RDBTable table,
      CodecBuffer prefix) {
    super(iterator, table, prefix);
    this.name = table != null ? table.getName() : null;
    this.inUseBuffers = Sets.newConcurrentHashSet();
    seekToFirst();
  }

  void assertOpen() {
    Preconditions.assertTrue(!closed.get(), "Already closed");
  }

  private RawKeyValue<Buffer> getBuffers(Set<RawKeyValue<Buffer>> inUseSet) {
    Buffer keyBuffer = new Buffer(
        new CodecBuffer.Capacity(name + "-iterator-key", 1 << 10),
        buffer -> getRocksDBIterator().get().key(buffer));
    Buffer valueBuffer = new Buffer(
        new CodecBuffer.Capacity(name + "-iterator-value", 4 << 10),
        buffer -> getRocksDBIterator().get().value(buffer));
    RawKeyValue<Buffer> kv = new RawKeyValue<>(keyBuffer, valueBuffer);
    inUseSet.add(kv);
    return kv;
  }

  private ReferenceCountedObject<RawKeyValue<CodecBuffer>> getReferenceCountedBuffer(
      RawKeyValue<Buffer> key, Set<RawKeyValue<Buffer>> inUseSet,
      Function<RawKeyValue<Buffer>, RawKeyValue<CodecBuffer>> transformer) {
    RawKeyValue<CodecBuffer> value = transformer.apply(key);
    return ReferenceCountedObject.wrap(value, () -> {
    }, completelyReleased -> {
      if (!completelyReleased) {
        return;
      }
      boolean exists = inUseSet.remove(key);
      if (exists) {
        key.getKey().release();
        value.getKey().release();
      }
    });
  }

  @Override
  ReferenceCountedObject<RawKeyValue<CodecBuffer>> getKeyValue() {
    assertOpen();
    RawKeyValue<Buffer> kvBuffer = getBuffers(inUseBuffers);
    Function<RawKeyValue<Buffer>, RawKeyValue<CodecBuffer>> transformer =
        kv -> new RawKeyValue<>(kv.getKey().getFromDb(), kv.getValue().getFromDb());
    return getReferenceCountedBuffer(kvBuffer, inUseBuffers, transformer);
  }

  @Override
  void seek0(CodecBuffer key) {
    assertOpen();
    getRocksDBIterator().get().seek(key.asReadOnlyByteBuffer());
  }

  @Override
  void delete(CodecBuffer key) throws IOException {
    assertOpen();
    getRocksDBTable().delete(key.asReadOnlyByteBuffer());
  }

  @Override
  boolean startsWithPrefix(CodecBuffer key) {
    assertOpen();
    final CodecBuffer prefix = getPrefix();
    if (prefix == null) {
      return true;
    }
    if (key == null) {
      return false;
    }
    return key.startsWith(prefix);
  }

  private <V> void release(Set<V> inUseSet, Function<V, Void> releaser) {
    for (V inUseValue : inUseSet) {
      boolean exists = inUseSet.remove(inUseValue);
      if (exists) {
        releaser.apply(inUseValue);
      }
    }
  }

  @Override
  public synchronized void close() {
    if (closed.compareAndSet(false, true)) {
      super.close();
      Optional.ofNullable(getPrefix()).ifPresent(CodecBuffer::release);
      release(inUseBuffers, kv -> {
        kv.getKey().release();
        kv.getValue().release();
        return null;
      });
    }
  }
}
