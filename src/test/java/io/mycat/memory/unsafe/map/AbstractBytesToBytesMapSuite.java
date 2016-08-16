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

package io.mycat.memory.unsafe.map;

import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.array.ByteArrayMethods;
import io.mycat.memory.unsafe.memory.TestMemoryManager;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.storage.DataNodeDiskManager;
import io.mycat.memory.unsafe.storage.SerializerManager;
import io.mycat.memory.unsafe.utils.JavaUtils;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Answers.RETURNS_SMART_NULLS;


public abstract class AbstractBytesToBytesMapSuite {

  private final Random rand = new Random(42);
  MycatPropertyConf conf = new MycatPropertyConf()
          .set("mycat.memory.offHeap.enabled", "" + useOffHeapMemoryAllocator())
          .set("mycat.memory.offHeap.size", "256mb");
  private TestMemoryManager memoryManager =
          new TestMemoryManager(conf
          );

  private DataNodeMemoryManager dataNodeMemoryManager =
          new DataNodeMemoryManager(memoryManager,0);

  private SerializerManager serializerManager = new SerializerManager();
  private static final long PAGE_SIZE_BYTES = 1L << 26; // 64 megabytes

  final LinkedList<File> spillFilesCreated = new LinkedList<File>();
  File tempDir;

  DataNodeDiskManager blockManager = new DataNodeDiskManager(conf,true,serializerManager);


/*
  private static final class CompressStream extends AbstractFunction1<OutputStream, OutputStream> {
    @Override
    public OutputStream apply(OutputStream stream) {
      return stream;
    }
  }
*/
  @Before
  public void setup() {
  }

  @After
  public void tearDown() throws IOException {
    //Utils.deleteRecursively(tempDir);
    //tempDir = null;

    if (dataNodeMemoryManager != null) {
      Assert.assertEquals(0L, dataNodeMemoryManager.cleanUpAllAllocatedMemory());
      long leakedMemory = dataNodeMemoryManager.getMemoryConsumptionForThisConnection();
      dataNodeMemoryManager = null;
      Assert.assertEquals(0L, leakedMemory);
    }
  }

  protected abstract boolean useOffHeapMemoryAllocator();

  private static byte[] getByteArray(Object base, long offset, int size) {
    final byte[] arr = new byte[size];
    Platform.copyMemory(base, offset, arr, Platform.BYTE_ARRAY_OFFSET, size);
    return arr;
  }

  private byte[] getRandomByteArray(int numWords) {
    Assert.assertTrue(numWords >= 0);
    final int lengthInBytes = numWords * 8;
    final byte[] bytes = new byte[lengthInBytes];
    rand.nextBytes(bytes);
    return bytes;
  }

  /**
   * Fast equality checking for byte arrays, since these comparisons are a bottleneck
   * in our stress tests.
   */
  private static boolean arrayEquals(
      byte[] expected,
      Object base,
      long offset,
      long actualLengthBytes) {
    return (actualLengthBytes == expected.length) && ByteArrayMethods.arrayEquals(
      expected,
      Platform.BYTE_ARRAY_OFFSET,
      base,
      offset,
      expected.length
    );
  }

  @Test
  public void emptyMap() {
    BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, 64, PAGE_SIZE_BYTES);
    try {
      Assert.assertEquals(0, map.numKeys());
      final int keyLengthInWords = 10;
      final int keyLengthInBytes = keyLengthInWords * 8;
      final byte[] key = getRandomByteArray(keyLengthInWords);
      Assert.assertFalse(map.lookup(key, Platform.BYTE_ARRAY_OFFSET, keyLengthInBytes).isDefined());
      Assert.assertFalse(map.iterator().hasNext());
    } finally {
      map.free();
    }
  }

  @Test
  public void setAndRetrieveAKey() {
    BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, 64, PAGE_SIZE_BYTES);
    final int recordLengthWords = 10;
    final int recordLengthBytes = recordLengthWords * 8;
    final byte[] keyData = getRandomByteArray(recordLengthWords);
    final byte[] valueData = getRandomByteArray(recordLengthWords);
    try {
      final BytesToBytesMap.Location loc =
        map.lookup(keyData, Platform.BYTE_ARRAY_OFFSET, recordLengthBytes);
      Assert.assertFalse(loc.isDefined());
      Assert.assertTrue(loc.append(
        keyData,
        Platform.BYTE_ARRAY_OFFSET,
        recordLengthBytes,
        valueData,
        Platform.BYTE_ARRAY_OFFSET,
        recordLengthBytes
      ));
      // After storing the key and value, the other location methods should return results that
      // reflect the result of this store without us having to call lookup() again on the same key.
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());
      Assert.assertArrayEquals(keyData,
        getByteArray(loc.getKeyBase(), loc.getKeyOffset(), recordLengthBytes));
      Assert.assertArrayEquals(valueData,
        getByteArray(loc.getValueBase(), loc.getValueOffset(), recordLengthBytes));

      // After calling lookup() the location should still point to the correct data.
      Assert.assertTrue(
        map.lookup(keyData, Platform.BYTE_ARRAY_OFFSET, recordLengthBytes).isDefined());
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());
      Assert.assertArrayEquals(keyData,
        getByteArray(loc.getKeyBase(), loc.getKeyOffset(), recordLengthBytes));
      Assert.assertArrayEquals(valueData,
        getByteArray(loc.getValueBase(), loc.getValueOffset(), recordLengthBytes));

      try {
        Assert.assertTrue(loc.append(
          keyData,
          Platform.BYTE_ARRAY_OFFSET,
          recordLengthBytes,
          valueData,
          Platform.BYTE_ARRAY_OFFSET,
          recordLengthBytes
        ));
        Assert.fail("Should not be able to set a new value for a key");
      } catch (AssertionError e) {
        // Expected exception; do nothing.
      }
    } finally {
      map.free();
    }
  }

  private void iteratorTestBase(boolean destructive) throws Exception {
    final int size = 4096;
    BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, size / 2, PAGE_SIZE_BYTES);
    try {
      for (long i = 0; i < size; i++) {
        final long[] value = new long[] { i };
        final BytesToBytesMap.Location loc =
          map.lookup(value, Platform.LONG_ARRAY_OFFSET, 8);
        Assert.assertFalse(loc.isDefined());
        // Ensure that we store some zero-length keys
        if (i % 5 == 0) {
          Assert.assertTrue(loc.append(
            null,
            Platform.LONG_ARRAY_OFFSET,
            0,
            value,
            Platform.LONG_ARRAY_OFFSET,
            8
          ));
        } else {
          Assert.assertTrue(loc.append(
            value,
            Platform.LONG_ARRAY_OFFSET,
            8,
            value,
            Platform.LONG_ARRAY_OFFSET,
            8
          ));
        }
      }
      final BitSet valuesSeen = new BitSet(size);
      final Iterator<BytesToBytesMap.Location> iter;
      if (destructive) {
        iter = map.destructiveIterator();
      } else {
        iter = map.iterator();
      }
      int numPages = map.getNumDataPages();
      int countFreedPages = 0;
      while (iter.hasNext()) {
        final BytesToBytesMap.Location loc = iter.next();
        Assert.assertTrue(loc.isDefined());
        final long value = Platform.getLong(loc.getValueBase(), loc.getValueOffset());
        final long keyLength = loc.getKeyLength();
        if (keyLength == 0) {
          Assert.assertTrue("value " + value + " was not divisible by 5", value % 5 == 0);
        } else {
          final long key = Platform.getLong(loc.getKeyBase(), loc.getKeyOffset());
          Assert.assertEquals(value, key);
        }
        valuesSeen.set((int) value);
        if (destructive) {
          // The iterator moves onto next page and frees previous page
          if (map.getNumDataPages() < numPages) {
            numPages = map.getNumDataPages();
            countFreedPages++;
          }
        }
      }
      if (destructive) {
        // Latest page is not freed by iterator but by map itself
        Assert.assertEquals(countFreedPages, numPages - 1);
      }
      Assert.assertEquals(size, valuesSeen.cardinality());
    } finally {
      map.free();
    }
  }

  @Test
  public void iteratorTest() throws Exception {
    iteratorTestBase(false);
  }

  @Test
  public void destructiveIteratorTest() throws Exception {
    iteratorTestBase(true);
  }

  @Test
  public void iteratingOverDataPagesWithWastedSpace() throws Exception {
    final int NUM_ENTRIES = 1000 * 1000;
    final int KEY_LENGTH = 24;
    final int VALUE_LENGTH = 40;
    final BytesToBytesMap map =
      new BytesToBytesMap(dataNodeMemoryManager, NUM_ENTRIES, PAGE_SIZE_BYTES);
    // Each record will take 8 + 24 + 40 = 72 bytes of space in the data page. Our 64-megabyte
    // pages won't be evenly-divisible by records of this size, which will cause us to waste some
    // space at the end of the page. This is necessary in order for us to take the end-of-record
    // handling branch in iterator().
    try {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        final long[] key = new long[] { i, i, i };  // 3 * 8 = 24 bytes
        final long[] value = new long[] { i, i, i, i, i }; // 5 * 8 = 40 bytes

        final BytesToBytesMap.Location loc = map.lookup(
          key,
          Platform.LONG_ARRAY_OFFSET,
          KEY_LENGTH
        );

        Assert.assertFalse(loc.isDefined());
        Assert.assertTrue(loc.append(
          key,
          Platform.LONG_ARRAY_OFFSET,
          KEY_LENGTH,
          value,
          Platform.LONG_ARRAY_OFFSET,
          VALUE_LENGTH
        ));
      }
      Assert.assertEquals(2, map.getNumDataPages());

      final BitSet valuesSeen = new BitSet(NUM_ENTRIES);
      final Iterator<BytesToBytesMap.Location> iter = map.iterator();
      final long[] key = new long[KEY_LENGTH / 8];
      final long[] value = new long[VALUE_LENGTH / 8];
      while (iter.hasNext()) {
        final BytesToBytesMap.Location loc = iter.next();
        Assert.assertTrue(loc.isDefined());
        Assert.assertEquals(KEY_LENGTH, loc.getKeyLength());
        Assert.assertEquals(VALUE_LENGTH, loc.getValueLength());
        Platform.copyMemory(
          loc.getKeyBase(),
          loc.getKeyOffset(),
          key,
          Platform.LONG_ARRAY_OFFSET,
          KEY_LENGTH
        );
        Platform.copyMemory(
          loc.getValueBase(),
          loc.getValueOffset(),
          value,
          Platform.LONG_ARRAY_OFFSET,
          VALUE_LENGTH
        );
        for (long j : key) {
          Assert.assertEquals(key[0], j);
        }
        for (long j : value) {
          Assert.assertEquals(key[0], j);
        }
        valuesSeen.set((int) key[0]);
      }
      Assert.assertEquals(NUM_ENTRIES, valuesSeen.cardinality());
    } finally {
      map.free();
    }
  }

  @Test
  public void randomizedStressTest() {
    final int size = 65536;
    // Java arrays' hashCodes() aren't based on the arrays' contents, so we need to wrap arrays
    // into ByteBuffers in order to use them as keys here.
    final Map<ByteBuffer, byte[]> expected = new HashMap<ByteBuffer,byte[]>();
    final BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, size, PAGE_SIZE_BYTES);
    try {
      // Fill the map to 90% full so that we can trigger probing
      for (int i = 0; i < size * 0.9; i++) {
        final byte[] key = getRandomByteArray(rand.nextInt(10) + 1);
        final byte[] value = getRandomByteArray(rand.nextInt(10) + 1);

        if (!expected.containsKey(ByteBuffer.wrap(key))) {
          expected.put(ByteBuffer.wrap(key), value);
          final BytesToBytesMap.Location loc = map.lookup(
            key,
            Platform.BYTE_ARRAY_OFFSET,
            key.length
          );
          Assert.assertFalse(loc.isDefined());
          Assert.assertTrue(loc.append(
            key,
            Platform.BYTE_ARRAY_OFFSET,
            key.length,
            value,
            Platform.BYTE_ARRAY_OFFSET,
            value.length
          ));
          // After calling putNewKey, the following should be true, even before calling
          // lookup():
          Assert.assertTrue(loc.isDefined());
          Assert.assertEquals(key.length, loc.getKeyLength());
          Assert.assertEquals(value.length, loc.getValueLength());
          Assert.assertTrue(arrayEquals(key, loc.getKeyBase(), loc.getKeyOffset(), key.length));
          Assert.assertTrue(
            arrayEquals(value, loc.getValueBase(), loc.getValueOffset(), value.length));
        }
      }

/**
      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        final byte[] key = JavaUtils.bufferToArray(entry.getKey());
        final byte[] value = entry.getValue();
        final BytesToBytesMap.Location loc =
          map.lookup(key, Platform.BYTE_ARRAY_OFFSET, key.length);
        Assert.assertTrue(loc.isDefined());
        Assert.assertTrue(
          arrayEquals(key, loc.getKeyBase(), loc.getKeyOffset(), loc.getKeyLength()));
        Assert.assertTrue(
          arrayEquals(value, loc.getValueBase(), loc.getValueOffset(), loc.getValueLength()));
 }
*/
    } finally {
      map.free();
    }
  }

  @Test
  public void randomizedTestWithRecordsLargerThanPageSize() {
    final long pageSizeBytes = 128;
    final BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, 64, pageSizeBytes);
    // Java arrays' hashCodes() aren't based on the arrays' contents, so we need to wrap arrays
    // into ByteBuffers in order to use them as keys here.
    final Map<ByteBuffer, byte[]> expected = new HashMap<ByteBuffer, byte[]>();
    try {
      for (int i = 0; i < 1000; i++) {
        final byte[] key = getRandomByteArray(rand.nextInt(128));
        final byte[] value = getRandomByteArray(rand.nextInt(128));
        if (!expected.containsKey(ByteBuffer.wrap(key))) {
          expected.put(ByteBuffer.wrap(key), value);
          final BytesToBytesMap.Location loc = map.lookup(
            key,
            Platform.BYTE_ARRAY_OFFSET,
            key.length
          );
          Assert.assertFalse(loc.isDefined());
          Assert.assertTrue(loc.append(
            key,
            Platform.BYTE_ARRAY_OFFSET,
            key.length,
            value,
            Platform.BYTE_ARRAY_OFFSET,
            value.length
          ));
          // After calling putNewKey, the following should be true, even before calling
          // lookup():
          Assert.assertTrue(loc.isDefined());
          Assert.assertEquals(key.length, loc.getKeyLength());
          Assert.assertEquals(value.length, loc.getValueLength());
          Assert.assertTrue(arrayEquals(key, loc.getKeyBase(), loc.getKeyOffset(), key.length));
          Assert.assertTrue(
            arrayEquals(value, loc.getValueBase(), loc.getValueOffset(), value.length));
        }
      }
/**
      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        final byte[] key = JavaUtils.bufferToArray(entry.getKey());
        final byte[] value = entry.getValue();
        final BytesToBytesMap.Location loc =
          map.lookup(key, Platform.BYTE_ARRAY_OFFSET, key.length);
        Assert.assertTrue(loc.isDefined());
        Assert.assertTrue(
          arrayEquals(key, loc.getKeyBase(), loc.getKeyOffset(), loc.getKeyLength()));
        Assert.assertTrue(
          arrayEquals(value, loc.getValueBase(), loc.getValueOffset(), loc.getValueLength()));
      }
*/
    } finally {
      map.free();
    }
  }

  @Test
  public void failureToAllocateFirstPage() {
    memoryManager.limit(1024);  // longArray
    BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, 1, PAGE_SIZE_BYTES);
    try {
      final long[] emptyArray = new long[0];
      final BytesToBytesMap.Location loc =
        map.lookup(emptyArray, Platform.LONG_ARRAY_OFFSET, 0);
      Assert.assertFalse(loc.isDefined());
      Assert.assertFalse(loc.append(
        emptyArray, Platform.LONG_ARRAY_OFFSET, 0, emptyArray, Platform.LONG_ARRAY_OFFSET, 0));
    } finally {
      map.free();
    }
  }


  @Test
  public void failureToGrow() {
    BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, 1, 1024);
    try {
      boolean success = true;
      int i;
      for (i = 0; i < 127; i++) {
        if (i > 0) {
          memoryManager.limit(0);
        }
        final long[] arr = new long[]{i};
        final BytesToBytesMap.Location loc = map.lookup(arr, Platform.LONG_ARRAY_OFFSET, 8);
        success =
          loc.append(arr, Platform.LONG_ARRAY_OFFSET, 8, arr, Platform.LONG_ARRAY_OFFSET, 8);
        if (!success) {
          break;
        }
      }
      Assert.assertThat(i, greaterThan(0));
      Assert.assertFalse(success);
    } finally {
      map.free();
    }
  }

  @Test
  public void spillInIterator() throws IOException {
    BytesToBytesMap map = new BytesToBytesMap(
            dataNodeMemoryManager, blockManager, serializerManager, 1, 0.75, 1024, false);
    try {
      int i;
      for (i = 0; i < 1024; i++) {
        final long[] arr = new long[]{i};
        final BytesToBytesMap.Location loc = map.lookup(arr, Platform.LONG_ARRAY_OFFSET, 8);
        loc.append(arr, Platform.LONG_ARRAY_OFFSET, 8, arr, Platform.LONG_ARRAY_OFFSET, 8);
      }
      BytesToBytesMap.MapIterator iter = map.iterator();
      for (i = 0; i < 100; i++) {
        iter.next();
      }
      // Non-destructive iterator is not spillable
      Assert.assertEquals(0, iter.spill(1024L * 10));
      for (i = 100; i < 1024; i++) {
        iter.next();
      }

      BytesToBytesMap.MapIterator iter2 = map.destructiveIterator();
      for (i = 0; i < 100; i++) {
        iter2.next();
      }
      Assert.assertTrue(iter2.spill(1024) >= 1024);
      for (i = 100; i < 1024; i++) {
        iter2.next();
      }
      assertFalse(iter2.hasNext());
    } finally {
      map.free();
      for (File spillFile : spillFilesCreated) {
        assertFalse("Spill file " + spillFile.getPath() + " was not cleaned up",
          spillFile.exists());
      }
    }
  }

  @Test
  public void multipleValuesForSameKey() {
    BytesToBytesMap map =
      new BytesToBytesMap(dataNodeMemoryManager, blockManager, serializerManager, 1, 0.75, 1024, false);
    try {
      int i;
      for (i = 0; i < 1024; i++) {
        final long[] arr = new long[]{i};
        map.lookup(arr, Platform.LONG_ARRAY_OFFSET, 8)
          .append(arr, Platform.LONG_ARRAY_OFFSET, 8, arr, Platform.LONG_ARRAY_OFFSET, 8);
      }
      assert map.numKeys() == 1024;
      assert map.numValues() == 1024;
      for (i = 0; i < 1024; i++) {
        final long[] arr = new long[]{i};
        map.lookup(arr, Platform.LONG_ARRAY_OFFSET, 8)
          .append(arr, Platform.LONG_ARRAY_OFFSET, 8, arr, Platform.LONG_ARRAY_OFFSET, 8);
      }
      assert map.numKeys() == 1024;
      assert map.numValues() == 2048;
      for (i = 0; i < 1024; i++) {
        final long[] arr = new long[]{i};
        final BytesToBytesMap.Location loc = map.lookup(arr, Platform.LONG_ARRAY_OFFSET, 8);
        assert loc.isDefined();
        assert loc.nextValue();
        assert !loc.nextValue();
      }
      BytesToBytesMap.MapIterator iter = map.iterator();
      for (i = 0; i < 2048; i++) {
        assert iter.hasNext();
        final BytesToBytesMap.Location loc = iter.next();
        assert loc.isDefined();
      }
    } finally {
      map.free();
    }
  }

  @Test
  public void initialCapacityBoundsChecking() {
    try {
      new BytesToBytesMap(dataNodeMemoryManager, 0, PAGE_SIZE_BYTES);
      Assert.fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      // expected exception
    }

    try {
      new BytesToBytesMap(
              dataNodeMemoryManager,
        BytesToBytesMap.MAX_CAPACITY + 1,
        PAGE_SIZE_BYTES);
      Assert.fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
  }

  @Test
  public void testPeakMemoryUsed() {
    final long recordLengthBytes = 32;
    final long pageSizeBytes = 256 + 8; // 8 bytes for end-of-page marker
    final long numRecordsPerPage = (pageSizeBytes - 8) / recordLengthBytes;
    final BytesToBytesMap map = new BytesToBytesMap(dataNodeMemoryManager, 1024, pageSizeBytes);

    // Since BytesToBytesMap is append-only, we expect the total memory consumption to be
    // monotonically increasing. More specifically, every time we allocate a new page it
    // should increase by exactly the size of the page. In this regard, the memory usage
    // at any given time is also the peak memory used.
    long previousPeakMemory = map.getPeakMemoryUsedBytes();
    long newPeakMemory;
    try {
      for (long i = 0; i < numRecordsPerPage * 10; i++) {
        final long[] value = new long[]{i};
        map.lookup(value, Platform.LONG_ARRAY_OFFSET, 8).append(
          value,
          Platform.LONG_ARRAY_OFFSET,
          8,
          value,
          Platform.LONG_ARRAY_OFFSET,
          8);
        newPeakMemory = map.getPeakMemoryUsedBytes();
        if (i % numRecordsPerPage == 0) {
          // We allocated a new page for this record, so peak memory should change
          assertEquals(previousPeakMemory + pageSizeBytes, newPeakMemory);
        } else {
          assertEquals(previousPeakMemory, newPeakMemory);
        }
        previousPeakMemory = newPeakMemory;
      }

      // Freeing the map should not change the peak memory
      map.free();
      newPeakMemory = map.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);

    } finally {
      map.free();
    }
  }

}
