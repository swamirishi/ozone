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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

class CopyCodecBufferRawIterator extends RawIterator<CodecBuffer, CodecBuffer, CodecBuffer> {
  private Buffer keyBuffer;
  private Buffer valueBuffer;
  private AtomicReference<CodecBuffer> keyBufferRef;
  private AtomicReference<CodecBuffer> valueBufferRef;

  CopyCodecBufferRawIterator(TableIterator<CodecBuffer, Table.KeyValue<CodecBuffer, CodecBuffer>> rawIterator) {
    super(rawIterator);
    keyBufferRef = new AtomicReference<>();
    valueBufferRef = new AtomicReference<>();
    this.keyBuffer = new Buffer(
        new CodecBuffer.Capacity("CopyCodec-iterator-key-buffer", 1 << 10),
        buffer -> {
          if (buffer.remaining() >= keyBufferRef.get().readableBytes()) {
            buffer.put(keyBufferRef.get().asReadOnlyByteBuffer());
          }
          return keyBufferRef.get().readableBytes();
        });
    this.valueBuffer = new Buffer(
        new CodecBuffer.Capacity("CopyCodec-iterator-value-buffer", 4 << 10),
        buffer -> {
          if (buffer.remaining() >= valueBufferRef.get().readableBytes()) {
            buffer.put(valueBufferRef.get().asReadOnlyByteBuffer());
          }
          return valueBufferRef.get().readableBytes();
        });
    this.keyBufferRef = new AtomicReference<>();
    this.valueBufferRef = new AtomicReference<>();
  }

  @Override
  AutoCloseSupplier<CodecBuffer> convert(CodecBuffer codecBuffer) {
    return () -> codecBuffer;
  }

  @Override
  Table.KeyValue<CodecBuffer, CodecBuffer> convert(Table.KeyValue<CodecBuffer, CodecBuffer> raw) throws IOException {
    keyBufferRef.set(raw.getKey());
    CodecBuffer key = keyBuffer.getFromDb();
    valueBufferRef.set(raw.getValue());
    CodecBuffer value = valueBuffer.getFromDb();
    return Table.newKeyValue(key, value, value.readableBytes());
  }

  @Override
  public void close() throws IOException {
    keyBuffer.release();
    valueBuffer.release();
  }
}
