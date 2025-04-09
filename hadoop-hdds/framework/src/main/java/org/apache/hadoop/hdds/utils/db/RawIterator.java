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

/**
 * A {@link Table.KeyValueIterator} backed by a raw iterator.
 *
 * @param <RAW> The raw type.
 */
abstract class RawIterator<RAW, K, V>
    implements Table.KeyValueIterator<K, V> {
  private final TableIterator<RAW, Table.KeyValue<RAW, RAW>> rawIterator;

  RawIterator(TableIterator<RAW, Table.KeyValue<RAW, RAW>> rawIterator) {
    this.rawIterator = rawIterator;
  }

  /**
   * Covert the given key to the {@link RAW} type.
   */
  abstract AutoCloseSupplier<RAW> convert(K k) throws IOException;

  /**
   * Covert the given {@link Table.KeyValue}
   * from ({@link RAW}, {@link RAW}) to ({@link K}, {@link V}).
   */
  abstract Table.KeyValue<K, V> convert(Table.KeyValue<RAW, RAW> raw)
      throws IOException;

  @Override
  public void seekToFirst() {
    rawIterator.seekToFirst();
  }

  @Override
  public void seekToLast() {
    rawIterator.seekToLast();
  }

  @Override
  public Table.KeyValue<K, V> seek(K k) throws IOException {
    try (AutoCloseSupplier<RAW> rawKey = convert(k)) {
      final Table.KeyValue<RAW, RAW> result = rawIterator.seek(rawKey.get());
      return result == null ? null : convert(result);
    }
  }

  @Override
  public void close() throws IOException {
    rawIterator.close();
  }

  @Override
  public boolean hasNext() {
    return rawIterator.hasNext();
  }

  @Override
  public Table.KeyValue<K, V> next() {
    try {
      return convert(rawIterator.next());
    } catch (IOException e) {
      throw new IllegalStateException("Failed next()", e);
    }
  }

  @Override
  public void removeFromDB() throws IOException {
    rawIterator.removeFromDB();
  }
}
