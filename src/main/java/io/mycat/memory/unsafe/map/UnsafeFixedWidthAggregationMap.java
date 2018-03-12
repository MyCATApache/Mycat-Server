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

import io.mycat.MycatServer;
import io.mycat.memory.unsafe.KVIterator;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.hash.Murmur3_x86_32;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.row.StructType;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.memory.unsafe.utils.sort.UnsafeKVExternalSorter;
import org.apache.log4j.Logger;


import java.io.IOException;

/**
 * Modify by zagnix ,add put find func
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 * This map supports a maximum of 2 billion keys.
 */
public final class UnsafeFixedWidthAggregationMap {
    private static Logger LOGGER = Logger.getLogger(UnsafeFixedWidthAggregationMap.class);

  /**
   * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
   * map, we copy this buffer and use it as the value.
   */
  private final byte[] emptyAggregationBuffer;
  private final StructType aggregationBufferSchema;
  private final StructType groupingKeySchema;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   */
  private final BytesToBytesMap map;

  /**
   * Re-used pointer to the current aggregation buffer
   */
  private final UnsafeRow currentAggregationBuffer;

  private final boolean enablePerfMetrics;

  private final static int SEED = 42;

  /**
   * @return true if UnsafeFixedWidthAggregationMap supports aggregation buffers with the given
   *         schema, false otherwise.
   */
  public static boolean supportsAggregationBufferSchema(StructType schema) {
    return true;
  }

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param emptyAggregationBuffer the default value for new keys (a "zero" of the agg. function)
   * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
   * @param groupingKeySchema the schema of the grouping key, used for row conversion.
   * @param dataNodeMemoryManager the memory manager used to allocate our Unsafe memory structures.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public UnsafeFixedWidthAggregationMap(
      UnsafeRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      DataNodeMemoryManager dataNodeMemoryManager,
      int initialCapacity,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this.aggregationBufferSchema = aggregationBufferSchema;

    this.currentAggregationBuffer = new UnsafeRow(aggregationBufferSchema.length());
    this.groupingKeySchema = groupingKeySchema;
    this.map = new BytesToBytesMap(dataNodeMemoryManager,initialCapacity, pageSizeBytes, enablePerfMetrics);
    this.enablePerfMetrics = enablePerfMetrics;
    this.emptyAggregationBuffer = emptyAggregationBuffer.getBytes() ;
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object. If additional memory could not be allocated, then this method will
   * signal an error by returning null.
   */
  public UnsafeRow getAggregationBuffer(UnsafeRow groupingKey) {
    return getAggregationBufferFromUnsafeRow(groupingKey);
  }

  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key) {

    return getAggregationBufferFromUnsafeRow(key,
            Murmur3_x86_32.hashUnsafeWords(key.getBaseObject(),key.getBaseOffset(),
            key.getSizeInBytes(),SEED));
  }

  public boolean put(UnsafeRow key, UnsafeRow value){

    int hash =  Murmur3_x86_32.hashUnsafeWords(key.getBaseObject(),
            key.getBaseOffset(), key.getSizeInBytes(),SEED);

    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
            key.getBaseObject(),
            key.getBaseOffset(),
            key.getSizeInBytes(),
            hash);

    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      boolean putSucceeded = loc.append(
              key.getBaseObject(),
              key.getBaseOffset(),
              key.getSizeInBytes(),
              value.getBaseObject(),
              value.getBaseOffset(),
              value.getSizeInBytes());

      if (!putSucceeded) {
        return false;
      }
    }

    return true;
  }


  public boolean find(UnsafeRow key){

    int hash =  Murmur3_x86_32.hashUnsafeWords(key.getBaseObject(),key.getBaseOffset(), key.getSizeInBytes(),42);
    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(key.getBaseObject(),
            key.getBaseOffset(), key.getSizeInBytes(), hash);

    if (!loc.isDefined()) {
     return false;
    }
    return true;
  }


  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key, int hash) {
    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      key.getBaseObject(),
      key.getBaseOffset(),
      key.getSizeInBytes(),
      hash);

    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      boolean putSucceeded = loc.append(
        key.getBaseObject(),
        key.getBaseOffset(),
        key.getSizeInBytes(),
        emptyAggregationBuffer,
        Platform.BYTE_ARRAY_OFFSET,
        emptyAggregationBuffer.length
      );

      if (!putSucceeded) {
        return null;
      }
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    currentAggregationBuffer.pointTo(
      loc.getValueBase(),
      loc.getValueOffset(),
      loc.getValueLength()
    );
    return currentAggregationBuffer;
  }

  /**
   * Returns an iterator over the keys and values in this map. This uses destructive iterator of
   * BytesToBytesMap. So it is illegal to call any other method on this map after `iterator()` has
   * been called.
   *
   * For efficiency, each call returns the same object.
   */
  public KVIterator<UnsafeRow,UnsafeRow> iterator() {
    return new KVIterator<UnsafeRow, UnsafeRow>() {

      private final BytesToBytesMap.MapIterator mapLocationIterator = map.iterator();

      private final UnsafeRow key = new UnsafeRow(groupingKeySchema.length());
      private final UnsafeRow value = new UnsafeRow(aggregationBufferSchema.length());

      @Override
      public boolean next() {
        if (mapLocationIterator.hasNext()) {
          final BytesToBytesMap.Location loc = mapLocationIterator.next();
            if (loc == null)
                return false;
          key.pointTo(
            loc.getKeyBase(),
            loc.getKeyOffset(),
            loc.getKeyLength()
          );
          value.pointTo(
            loc.getValueBase(),
            loc.getValueOffset(),
            loc.getValueLength()
          );
          return true;
        } else {
          return false;
        }
      }

      @Override
      public UnsafeRow getKey() {
        return key;
      }

      @Override
      public UnsafeRow getValue() {
        return value;
      }

      @Override
      public void close() {
      }
    };
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    return map.getPeakMemoryUsedBytes();
  }

  /**
   * Free the memory associated with this map. This is idempotent and can be called multiple times.
   */
  public void free() {
    map.free();
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void printPerfMetrics() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException("Perf metrics not enabled");
    }
    System.out.println("Average probes per lookup: " + map.getAverageProbesPerLookup());
    System.out.println("Number of hash collisions: " + map.getNumHashCollisions());
    System.out.println("Time spent resizing (ns): " + map.getTimeSpentResizingNs());
    System.out.println("Total memory consumption (bytes): " + map.getTotalMemoryConsumption());
  }

  /**
   * Sorts the map's records in place, spill them to disk, and returns an [[UnsafeKVExternalSorter]]
   *
   * Note that the map will be reset for inserting new records, and the returned sorter can NOT be
   * used to insert records.
   */
  public UnsafeKVExternalSorter destructAndCreateExternalSorter() throws IOException {
    return new UnsafeKVExternalSorter(
      groupingKeySchema,
      aggregationBufferSchema,
      MycatServer.getInstance().getMyCatMemory().getBlockManager(),
      MycatServer.getInstance().getMyCatMemory().getSerializerManager(),
      map.getPageSizeBytes(),
      map);
  }
}
