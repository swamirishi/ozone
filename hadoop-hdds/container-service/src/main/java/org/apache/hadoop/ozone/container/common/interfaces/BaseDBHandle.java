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
package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.ozone.container.metadata.AbstractStore;

import java.io.Closeable;

/**
 * DB handle abstract class.
 */
public abstract class BaseDBHandle<STORE extends AbstractStore> implements Closeable {

  private final STORE store;
  private final String containerDBPath;

  public BaseDBHandle(STORE store, String containerDBPath) {
    this.store = store;
    this.containerDBPath = containerDBPath;
  }

  public STORE getStore() {
    return this.store;
  }

  public String getContainerDBPath() {
    return this.containerDBPath;
  }

  public boolean cleanup() {
    return true;
  }

  @Override
  public String toString() {
    return "DBHandle{" +
        "containerDBPath='" + containerDBPath + '\'' +
        ", store=" + store +
        '}';
  }
}
