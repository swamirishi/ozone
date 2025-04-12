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

package org.apache.hadoop.hdds.scm.metadata;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Codec to serialize/deserialize {@link BigInteger}.
 */
public final class BigIntegerCodec implements Codec<BigInteger> {

  private static final Codec<BigInteger> INSTANCE = new BigIntegerCodec();
  private static final Comparator<BigInteger> COMPARATOR = (o1, o2) -> Objects.compare(o1, o2, BigInteger::compareTo);

  public static Codec<BigInteger> get() {
    return INSTANCE;
  }

  private BigIntegerCodec() {
    // singleton
  }

  @Override
  public Class<BigInteger> getTypeClass() {
    return BigInteger.class;
  }

  @Override
  public byte[] toPersistedFormat(BigInteger object) throws IOException {
    return object.toByteArray();
  }

  @Override
  public BigInteger fromPersistedFormat(byte[] rawData) throws IOException {
    return new BigInteger(rawData);
  }

  @Override
  public BigInteger copyObject(BigInteger object) {
    return object;
  }

  @Override
  public Comparator<BigInteger> comparator() {
    return COMPARATOR;
  }
}
