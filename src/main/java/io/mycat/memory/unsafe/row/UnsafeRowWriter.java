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

package io.mycat.memory.unsafe.row;


import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.array.ByteArrayMethods;
import io.mycat.memory.unsafe.bitset.BitSetMethods;

import java.math.BigDecimal;

/**
 * A helper class to write data into global row buffer using `UnsafeRow` format.
 *
 * It will remember the offset of row buffer which it starts to write, and move the cursor of row
 * buffer while writing.  If new data(can be the input record if this is the outermost writer, or
 * nested struct if this is an inner writer) comes, the starting cursor of row buffer may be
 * changed, so we need to call `UnsafeRowWriter.reset` before writing, to update the
 * `startingOffset` and clear out null bits.
 *
 * Note that if this is the outermost writer, which means we will always write from the very
 * beginning of the global row buffer, we don't need to update `startingOffset` and can just call
 * `zeroOutNullBytes` before writing new data.
 */
public class UnsafeRowWriter {

    private final BufferHolder holder;
    /**
     * 目标 buffer 开始写入位置
     * The offset of the global buffer where we start to write this row.
     */
    private int startingOffset;
    /**
     * fields 占用 Byte
     */
    private final int nullBitsSize;
    /**
     * 大小
     */
    private final int fixedSize;

    public UnsafeRowWriter(BufferHolder holder, int numFields) {
        this.holder = holder;
        this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
        this.fixedSize = nullBitsSize + 8 * numFields;
        this.startingOffset = holder.cursor;
    }

  /**
   * Resets the `startingOffset` according to the current cursor of row buffer, and clear out null
   * bits.  This should be called before we write a new nested struct to the row buffer.
   */
  public void reset() {
    this.startingOffset = holder.cursor;

    // grow the global buffer to make sure it has enough space to write fixed-length data.
    holder.grow(fixedSize);
    holder.cursor += fixedSize;

    zeroOutNullBytes();
  }

  /**
   * Clears out null bits.  This should be called before we write a new row to row buffer.
   */
  public void zeroOutNullBytes() {
    for (int i = 0; i < nullBitsSize; i += 8) {
      Platform.putLong(holder.buffer, startingOffset + i, 0L);
    }
  }

    /**
     * 填充 空隙位置 为 0
     * 为什么需要填充？因为 {@link ByteArrayMethods#roundNumberOfBytesToNearestWord(int)} 时，
     * 如果不被整除，则尾部有部分无法被我们的 字节数组 填充到，我们使用 0 进行填充，避免可能存在的内容存在在空隙位置。
     *
     * @param numBytes 字节数
     */
    private void zeroOutPaddingBytes(int numBytes) {
        if ((numBytes & 0x07) > 0) {
            Platform.putLong(holder.buffer, holder.cursor + ((numBytes >> 3) << 3), 0L);
        }
    }

  public BufferHolder holder() { return holder; }

  public boolean isNullAt(int ordinal) {
    return BitSetMethods.isSet(holder.buffer, startingOffset, ordinal);
  }

    public void setNullAt(int ordinal) {
        BitSetMethods.set(holder.buffer, startingOffset, ordinal);
        Platform.putLong(holder.buffer, getFieldOffset(ordinal), 0L);
    }

    /**
     * 获得 顺序 对应的 字节数组位置
     *
     * @param ordinal 顺序
     * @return 位置
     */
    public long getFieldOffset(int ordinal) {
        return startingOffset + nullBitsSize + 8 * ordinal;
    }

    /**
     * 设置 顺序位置 对应的 value 位置与大小
     *
     * @param ordinal 顺序
     * @param size 大小
     */
    public void setOffsetAndSize(int ordinal, long size) {
        setOffsetAndSize(ordinal, holder.cursor, size);
    }

    /**
     * 设置 顺序位置 对应的 value 位置与大小
     *
     * @param ordinal 顺序
     * @param currentCursor 当前 cursor
     * @param size 大小
     */
    public void setOffsetAndSize(int ordinal, long currentCursor, long size) {
        final long relativeOffset = currentCursor - startingOffset;
        final long fieldOffset = getFieldOffset(ordinal);
        final long offsetAndSize = (relativeOffset << 32) | size; // Long 8个字节对半拆成两半：relativeOffset、size。因此，relativeOffset 左移四个字节
        // 设置
        Platform.putLong(holder.buffer, fieldOffset, offsetAndSize);
    }

  // Do word alignment for this row and grow the row buffer if needed.
  // todo: remove this after we make unsafe array data word align.
  public void alignToWords(int numBytes) {
    final int remainder = numBytes & 0x07;

    if (remainder > 0) {
      final int paddingBytes = 8 - remainder;
      holder.grow(paddingBytes);

      for (int i = 0; i < paddingBytes; i++) {
        Platform.putByte(holder.buffer, holder.cursor, (byte) 0);
        holder.cursor++;
      }
    }
  }

  public void write(int ordinal, boolean value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(holder.buffer, offset, 0L);
    Platform.putBoolean(holder.buffer, offset, value);
  }

  public void write(int ordinal, byte value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(holder.buffer, offset, 0L);
    Platform.putByte(holder.buffer, offset, value);
  }

  public void write(int ordinal, short value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(holder.buffer, offset, 0L);
    Platform.putShort(holder.buffer, offset, value);
  }

  public void write(int ordinal, int value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(holder.buffer, offset, 0L);
    Platform.putInt(holder.buffer, offset, value);
  }

  public void write(int ordinal, long value) {
    Platform.putLong(holder.buffer, getFieldOffset(ordinal), value);
  }

  public void write(int ordinal, float value) {
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(holder.buffer, offset, 0L);
    Platform.putFloat(holder.buffer, offset, value);
  }

  public void write(int ordinal, double value) {
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    Platform.putDouble(holder.buffer, getFieldOffset(ordinal), value);
  }

    /**
     * 写入 字节数组 到 指定顺序
     *
     * @param ordinal 顺序
     * @param input 字节数组
     */
    public void write(int ordinal, byte[] input) {
        if (input == null) {
            return;
        }
        write(ordinal, input, 0, input.length);
    }

    /**
     * 写入 字节数组 到 指定顺序
     *
     * @param ordinal 顺序
     * @param input 字节数组
     * @param offset 字节数组-起始位置
     * @param numBytes 字节数组-写入字节数
     */
    public void write(int ordinal, byte[] input, int offset, int numBytes) {
        // 计算 numBytes 最接近的 8（Long） 字节长度
        final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);

        // grow the global buffer before writing data.
        holder.grow(roundedSize);

        // 填充 空隙位置 为 0
        zeroOutPaddingBytes(numBytes);

        // 写入 字节数组 Write the bytes to the variable length portion.
        Platform.copyMemory(input, Platform.BYTE_ARRAY_OFFSET + offset,
                holder.buffer, holder.cursor, numBytes);

        // 写入
        setOffsetAndSize(ordinal, numBytes);

        // move the cursor forward.
        holder.cursor += roundedSize;
    }

  	/**
	 * different from Spark, we use java BigDecimal here, 
	 * and we limit the max precision to be 38 because the bytes length limit to be 16
	 * 
	 * @param ordinal
	 * @param input
	 */
	public void write(int ordinal, BigDecimal input) {

		// grow the global buffer before writing data.
		holder.grow(16);

		// zero-out the bytes
		Platform.putLong(holder.buffer, holder.cursor, 0L);
		Platform.putLong(holder.buffer, holder.cursor + 8, 0L);

		// Make sure Decimal object has the same scale as DecimalType.
		// Note that we may pass in null Decimal object to set null for it.
		if (input == null) {
			BitSetMethods.set(holder.buffer, startingOffset, ordinal);
			// keep the offset for future update
			setOffsetAndSize(ordinal, 0L);
		} else {
			final byte[] bytes = input.unscaledValue().toByteArray();
			assert bytes.length <= 16;

			// Write the bytes to the variable length portion.
			Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, holder.buffer, holder.cursor, bytes.length);
			setOffsetAndSize(ordinal, bytes.length);
		}

		// move the cursor forward.
		holder.cursor += 16;
	}
  
}
