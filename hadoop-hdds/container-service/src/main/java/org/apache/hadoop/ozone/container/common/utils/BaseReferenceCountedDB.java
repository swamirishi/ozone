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

package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.ozone.container.common.interfaces.BaseDBHandle;
import org.apache.hadoop.ozone.container.metadata.AbstractStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class to implement reference counting over instances of a db handle.
 */
public class BaseReferenceCountedDB<STORE extends AbstractStore> extends BaseDBHandle<STORE> {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseReferenceCountedDB.class);
  private final AtomicInteger referenceCount;

  public BaseReferenceCountedDB(STORE store, String containerDBPath) {
    super(store, containerDBPath);
    this.referenceCount = new AtomicInteger(0);
  }

  public void incrementReference() {
    this.referenceCount.incrementAndGet();
    if (LOG.isTraceEnabled()) {
      LOG.trace("IncRef {} to refCnt {}, stackTrace: {}", getContainerDBPath(),
          referenceCount.get(), ExceptionUtils.getStackTrace(new Throwable()));
    }
  }

  public void decrementReference() {
    int refCount = this.referenceCount.decrementAndGet();
    Preconditions.checkArgument(refCount >= 0, "refCount:", refCount);
    if (LOG.isTraceEnabled()) {
      LOG.trace("DecRef {} to refCnt {}, stackTrace: {}", getContainerDBPath(),
          referenceCount.get(), ExceptionUtils.getStackTrace(new Throwable()));
    }
  }

  public boolean cleanup() {
    if (getStore() != null && getStore().isClosed()
        || referenceCount.get() == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Close {} refCnt {}", getContainerDBPath(),
            referenceCount.get());
      }
      try {
        getStore().stop();
        return true;
      } catch (Exception e) {
        LOG.error("Error closing DB. Container: " + getContainerDBPath(), e);
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    decrementReference();
  }

  /**
   * Returns if the underlying DB is closed. This call is threadsafe.
   * @return true if the DB is closed.
   */
  public boolean isClosed() {
    return getStore().isClosed();
  }
}
