/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.memory.unsafe.memory.mm;

import io.mycat.memory.unsafe.array.CharArray;
import io.mycat.memory.unsafe.array.LongArray;
import io.mycat.memory.unsafe.memory.MemoryBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
/**
 * An memory consumer of DataNodeMemoryManager, which support spilling.
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
public abstract class MemoryConsumer {
  private final Logger logger = LoggerFactory.getLogger(MemoryConsumer.class);

  protected final DataNodeMemoryManager dataNodeMemoryManager;
  private final long pageSize;
  protected long used;

  protected MemoryConsumer(DataNodeMemoryManager dataNodeMemoryManager, long pageSize) {
    this.dataNodeMemoryManager = dataNodeMemoryManager;
    this.pageSize = pageSize;
  }

  protected MemoryConsumer(DataNodeMemoryManager dataNodeMemoryManager) {
    this(dataNodeMemoryManager, dataNodeMemoryManager.pageSizeBytes());
  }

  /**
   * Returns the size of used memory in bytes.
   */
  public long getUsed() {
    return used;
  }

  /**
   * Force spill during building.
   *
   * For testing.
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * Spill some data to disk to release memory, which will be called by DataNodeMemoryManager
   * when there is not enough memory for the task.
   *
   * This should be implemented by subclass.
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   *
   * Note: today, this only frees Tungsten-managed pages.
   *
   * @param size the amount of memory should be released
   * @param trigger the MemoryConsumer that trigger this spilling
   * @return the amount of released memory in bytes
   * @throws IOException
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * Allocates a LongArray of `size`.
   */
  public LongArray allocateLongArray(long size) {
    long required = size * 8L;
    MemoryBlock page = dataNodeMemoryManager.allocatePage(required,this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        dataNodeMemoryManager.freePage(page, this);
      }
      dataNodeMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += required;
    return new LongArray(page);
  }

  /**
   * Frees a LongArray.
   */
  public void freeLongArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  public CharArray allocateCharArray(long size) {
    long required = size * 2L;
    MemoryBlock page = dataNodeMemoryManager.allocatePage(required,this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        dataNodeMemoryManager.freePage(page, this);
      }
      dataNodeMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += required;
    return new CharArray(page,this);
  }

  /**
   * Frees a CharArray.
   */
  public void freeCharArray(CharArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   *
   * Throws IOException if there is not enough memory.
   *
   * @throws OutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    MemoryBlock page = dataNodeMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        dataNodeMemoryManager.freePage(page,this);
      }
      dataNodeMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += page.size();
    return page;
  }

  /**
   * Free a memory block.
   */
  protected void freePage(MemoryBlock page) {
    used -= page.size();
    dataNodeMemoryManager.freePage(page, this);
  }

  /**
   * Allocates a heap memory of `size`.
   */
  public long acquireOnHeapMemory(long size) {
    long granted = 0;
    try {
      granted = dataNodeMemoryManager.acquireExecutionMemory(size, MemoryMode.ON_HEAP, this);
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
    }
    used += granted;
    return granted;
  }

  /**
   * Release N bytes of heap memory.
   */
  public void freeOnHeapMemory(long size) {
    dataNodeMemoryManager.releaseExecutionMemory(size, MemoryMode.ON_HEAP, this);
    used -= size;
  }
}
