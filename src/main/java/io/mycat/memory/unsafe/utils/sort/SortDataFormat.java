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

package io.mycat.memory.unsafe.utils.sort;

/**
 * Abstraction for sorting an arbitrary input buffer of data. This interface requires determining
 * the sort key for a given element index, as well as swapping elements and moving data from one
 * buffer to another.
 *
 * Example format: an array of numbers, where each element is also the key.
 * See [[KVArraySortDataFormat]] for a more exciting format.
 *
 * Note: Declaring and instantiating multiple subclasses of this class would prevent JIT inlining
 * overridden methods and hence decrease the shuffle performance.
 *
 * @tparam K Type of the sort key of each element
 * @tparam Buffer Internal data structure used by a particular format (e.g., Array[Int]).
 */
// TODO: Making Buffer a real trait would be a better abstraction, but adds some complexity.

public  abstract class SortDataFormat<K,Buffer> {

  /**
   * Creates a new mutable key for reuse. This should be implemented if you want to override
   * [[getKey(Buffer, Int, K)]].
   */
  public abstract K newKey();

  /** Return the sort key for the element at the given index. */
  protected abstract K getKey(Buffer data, int pos);

  /**
   * Returns the sort key for the element at the given index and reuse the input key if possible.
   * The default implementation ignores the reuse parameter and invokes [[getKey(Buffer, Int]].
   * If you want to override this method, you must implement [[newKey()]].
   */
  protected K getKey(Buffer data, int pos, K reuse) {
    return getKey(data, pos);
  }

  /** Swap two elements. */
  protected abstract  void swap(Buffer data, int pos0,int pos1);

  /** Copy a single element from src(srcPos) to dst(dstPos). */
  protected abstract  void  copyElement(Buffer src, int srcPos,Buffer dst ,int dstPos);

  /**
   * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
   * Overlapping ranges are allowed.
   */
  protected abstract  void  copyRange(Buffer src, int srcPos,Buffer dst, int dstPos, int length);

  /**
   * Allocates a Buffer that can hold up to 'length' elements.
   * All elements of the buffer should be considered invalid until data is explicitly copied in.
   */
  protected abstract  Buffer  allocate(int length);
}
