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

package io.mycat.memory.unsafe.sort;

import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.memory.MemoryBlock;
import io.mycat.memory.unsafe.memory.TestMemoryConsumer;
import io.mycat.memory.unsafe.memory.TestMemoryManager;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import io.mycat.memory.unsafe.utils.sort.*;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class UnsafeInMemorySorterSuite {

  protected boolean shouldUseRadixSort() { return true; }

  private static String getStringFromDataPage(Object baseObject,long baseOffset,int length) {
    final byte[] strBytes = new byte[length];
    Platform.copyMemory(baseObject,baseOffset,strBytes, Platform.BYTE_ARRAY_OFFSET,length);
    return new String(strBytes,StandardCharsets.UTF_8);
  }

  @Test
  public void testSortingEmptyInput() {
    final DataNodeMemoryManager memoryManager = new DataNodeMemoryManager(
      new TestMemoryManager(new MycatPropertyConf().set("mycat.memory.offHeap.enabled", "false")), 0);
    final TestMemoryConsumer consumer = new TestMemoryConsumer(memoryManager);
    final UnsafeInMemorySorter sorter = new UnsafeInMemorySorter(consumer,
      memoryManager,
      mock(RecordComparator.class),
      mock(PrefixComparator.class),
      100,
      shouldUseRadixSort(),true);
    final UnsafeSorterIterator iter = sorter.getSortedIterator();
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testSortingOnlyByIntegerPrefix() throws Exception {
    final String[] dataToSort = new String[] {
      "Boba",
      "Pearls",
      "Tapioca",
      "Taho",
      "Condensed Milk",
      "Jasmine",
      "Milk Tea",
      "Lychee",
      "Mango"
    };
    final DataNodeMemoryManager memoryManager = new DataNodeMemoryManager(
      new TestMemoryManager(new MycatPropertyConf().set("mycat.memory.offHeap.enabled","false")), 0);
    final TestMemoryConsumer consumer = new TestMemoryConsumer(memoryManager);
    final MemoryBlock dataPage = memoryManager.allocatePage(2048, null);

    final Object baseObject = dataPage.getBaseObject();

    // Write the records into the data page:
    long position = dataPage.getBaseOffset();

    for (String str : dataToSort) {
      final byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
      Platform.putInt(baseObject, position, strBytes.length);
      position += 4;
      Platform.copyMemory(strBytes,Platform.BYTE_ARRAY_OFFSET,baseObject, position, strBytes.length);
      position += strBytes.length;
    }

    // Since the key fits within the 8-byte prefix, we don't need to do any record comparison, so
    // use a dummy comparator
    final RecordComparator recordComparator = new RecordComparator() {
      @Override
      public int compare(
        Object leftBaseObject,
        long leftBaseOffset,
        Object rightBaseObject,
        long rightBaseOffset) {
        return 0;
      }
    };
    // Compute key prefixes based on the records' partition ids

    final HashPartitioner hashPartitioner = new HashPartitioner(4);

    // Use integer comparison for comparing prefixes (which are partition ids, in this case)
    final PrefixComparator prefixComparator = PrefixComparators.LONG;

    UnsafeInMemorySorter sorter = new UnsafeInMemorySorter(
            consumer,memoryManager,recordComparator,
            prefixComparator, dataToSort.length,
            shouldUseRadixSort(),true);

    // Given a page of records, insert those records into the sorter one-by-one:
    position = dataPage.getBaseOffset();
    System.out.println("(0)address = " + position);

    for (int i = 0; i < dataToSort.length; i++) {

      if (!sorter.hasSpaceForAnotherRecord()) {
        sorter.expandPointerArray(consumer.allocateLongArray(sorter.getMemoryUsage() / 8 * 2));
      }

      // position now points to the start of a record (which holds its length).
      final int recordLength = Platform.getInt(baseObject,position);

      final long address = memoryManager.encodePageNumberAndOffset(dataPage,position);


      final String str = getStringFromDataPage(baseObject,position+4,recordLength);

      final int partitionId = hashPartitioner.getPartition(str);
      System.out.println("(" + partitionId + "," + str + ")");

      sorter.insertRecord(address,partitionId);

      position += 4 + recordLength;
    }



    final UnsafeSorterIterator iter = sorter.getSortedIterator();

    int iterLength = 0;
    long prevPrefix = -1;

    Arrays.sort(dataToSort);



    while (iter.hasNext()) {
      iter.loadNext();

      final String str = getStringFromDataPage(iter.getBaseObject(), iter.getBaseOffset(), iter.getRecordLength());

      final long keyPrefix = iter.getKeyPrefix();

      assertThat(str, isIn(Arrays.asList(dataToSort)));
      assertThat(keyPrefix, greaterThanOrEqualTo(prevPrefix));

      prevPrefix = keyPrefix;

      iterLength++;
    }



    assertEquals(dataToSort.length, iterLength);
  }
}
