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

package io.mycat.memory.unsafe.array;


import io.mycat.memory.unsafe.Platform;

public class ByteArrayMethods {

  private ByteArrayMethods() {
    // Private constructor, since this class only contains static methods.
  }

  /** Returns the next number greater or equal num that is power of 2. */
  public static long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }

  public static int roundNumberOfBytesToNearestWord(int numBytes) {
    int remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
    if (remainder == 0) {
      return numBytes;
    } else {
      return numBytes + (8 - remainder);
    }
  }

  /**
   * Optimized byte array equality check for byte arrays.
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean arrayEquals(
      Object leftBase, long leftOffset, Object rightBase, long rightOffset, final long length) {
    int i = 0;
    while (i <= length - 8) {
      if (Platform.getLong(leftBase, leftOffset + i) !=
        Platform.getLong(rightBase, rightOffset + i)) {
        return false;
      }
      i += 8;
    }
    while (i < length) {
      if (Platform.getByte(leftBase, leftOffset + i) !=
        Platform.getByte(rightBase, rightOffset + i)) {
        return false;
      }
      i += 1;
    }
    return true;
  }
}
