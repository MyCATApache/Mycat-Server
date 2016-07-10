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

package io.mycat.memory.unsafe.memory;

import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.memory.mm.MemoryMode;
import io.mycat.memory.unsafe.memory.mm.ResultMergeMemoryManager;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import org.junit.Assert;
import org.junit.Test;

public class TaskMemoryManagerSuite {

  @Test
  public void leakedPageMemoryIsDetected() {
    final DataNodeMemoryManager manager = new DataNodeMemoryManager(
      new ResultMergeMemoryManager(
        new MycatPropertyConf().set("mycat.memory.offHeap.enabled", "false")
              .set("mycat.memory.offHeap.size","32768"),
              1,
        Long.MAX_VALUE
       ),
      0);
    manager.allocatePage(4096, null);  // leak memory
    Assert.assertEquals(4096, manager.getMemoryConsumptionForThisConnection());
    Assert.assertEquals(4096, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void encodePageNumberAndOffsetOffHeap() {
    final MycatPropertyConf conf = new MycatPropertyConf()
      .set("mycat.memory.offHeap.enabled", "true")
      .set("mycat.memory.offHeap.size", "1000");
    final DataNodeMemoryManager manager = new DataNodeMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryBlock dataPage = manager.allocatePage(256, null);
    // In off-heap mode, an offset is an absolute address that may require more than 51 bits to
    // encode. This map exercises that corner-case:
    final long offset = ((1L << DataNodeMemoryManager.OFFSET_BITS) + 10);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, offset);
    Assert.assertEquals(null, manager.getPage(encodedAddress));
    Assert.assertEquals(offset, manager.getOffsetInPage(encodedAddress));
  }

  @Test
  public void encodePageNumberAndOffsetOnHeap() {
    final DataNodeMemoryManager manager = new DataNodeMemoryManager(
      new TestMemoryManager(new MycatPropertyConf().set("mycat.memory.offHeap.enabled", "false")), 0);
    final MemoryBlock dataPage = manager.allocatePage(256, null);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, 64);
    Assert.assertEquals(dataPage.getBaseObject(), manager.getPage(encodedAddress));
    Assert.assertEquals(64, manager.getOffsetInPage(encodedAddress));
  }

  @Test
  public void cooperativeSpilling() throws InterruptedException {
    final TestMemoryManager memoryManager = new TestMemoryManager(new MycatPropertyConf());
    memoryManager.limit(100);
    final DataNodeMemoryManager manager = new DataNodeMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    c1.use(100);
    Assert.assertEquals(100, c1.getUsed());
    c2.use(100);
    Assert.assertEquals(100, c2.getUsed());
    Assert.assertEquals(0, c1.getUsed());  // spilled
    c1.use(100);
    Assert.assertEquals(100, c1.getUsed());
    Assert.assertEquals(0, c2.getUsed());  // spilled

    c1.use(50);
    Assert.assertEquals(50, c1.getUsed());  // spilled
    Assert.assertEquals(0, c2.getUsed());
    c2.use(50);
    Assert.assertEquals(50, c1.getUsed());
    Assert.assertEquals(50, c2.getUsed());

    c1.use(100);
    Assert.assertEquals(100, c1.getUsed());
    Assert.assertEquals(0, c2.getUsed());  // spilled

    c1.free(20);
    Assert.assertEquals(80, c1.getUsed());
    c2.use(10);
    Assert.assertEquals(80, c1.getUsed());
    Assert.assertEquals(10, c2.getUsed());
    c2.use(100);
    Assert.assertEquals(100, c2.getUsed());
    Assert.assertEquals(0, c1.getUsed());  // spilled

    c1.free(0);
    c2.free(100);
    Assert.assertEquals(0, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void offHeapConfigurationBackwardsCompatibility() {
    final MycatPropertyConf conf = new MycatPropertyConf()
    .set("mycat.memory.offHeap.enabled", "true")
      .set("mycat.memory.offHeap.size","1000");
    final DataNodeMemoryManager manager = new DataNodeMemoryManager(new TestMemoryManager(conf), 0);
    Assert.assertSame(MemoryMode.OFF_HEAP, manager.tungstenMemoryMode);
  }

}
