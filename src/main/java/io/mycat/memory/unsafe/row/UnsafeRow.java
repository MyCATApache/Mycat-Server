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


import io.mycat.backend.mysql.BufferUtil;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.array.ByteArrayMethods;
import io.mycat.memory.unsafe.bitset.BitSetMethods;
import io.mycat.memory.unsafe.hash.Murmur3_x86_32;
import io.mycat.memory.unsafe.types.UTF8String;
import io.mycat.net.FrontendConnection;
import io.mycat.net.mysql.MySQLPacket;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;


/**
 * Modify by zagnix 
 * An Unsafe implementation of Row which is backed by raw memory instead of Java objects.
 *
 * Each tuple has three parts: [null bit set] [values] [variable length portion]
 *
 * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
 * one bit per field.
 *
 * In the `values` region, we store one 8-byte word per field. For fields that hold fixed-length
 * primitive types, such as long, double, or int, we store the value directly in the word. For
 * fields with non-primitive or variable-length values, we store a relative offset (w.r.t. the
 * base address of the row) that points to the beginning of the variable-length field, and length
 * (they are combined into a long).
 *
 * Instances of `UnsafeRow` act as pointers to row data stored in this format.
 */
public final class UnsafeRow extends MySQLPacket {

  //////////////////////////////////////////////////////////////////////////////
  // Static methods
  //////////////////////////////////////////////////////////////////////////////

  public static int calculateBitSetWidthInBytes(int numFields) {
    return ((numFields + 63)/ 64) * 8;
  }

  public static int calculateFixedPortionByteSize(int numFields) {
    return 8 * numFields + calculateBitSetWidthInBytes(numFields);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Private fields and methods
  //////////////////////////////////////////////////////////////////////////////

  private Object baseObject;
  private long baseOffset;

  /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
  private int numFields;

  /** The size of this row's backing data, in bytes) */
  private int sizeInBytes;

  /** The width of the null tracking bit set, in bytes */
  private int bitSetWidthInBytes;

  private long getFieldOffset(int ordinal) {
    return baseOffset + bitSetWidthInBytes + ordinal * 8L;
  }

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should < " + numFields;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Public methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Construct a new UnsafeRow. The resulting row won't be usable until `pointTo()` has been called,
   * since the value returned by this constructor is equivalent to a null pointer.
   *
   * @param numFields the number of fields in this row
   */
  public UnsafeRow(int numFields) {
    this.numFields = numFields;
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
  }

  // for serializer
  public UnsafeRow() {}

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  public int numFields() { return numFields; }

  /**
   * Update this UnsafeRow to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this row's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    assert numFields >= 0 : "numFields (" + numFields + ") should >= 0";
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  /**
   * Update this UnsafeRow to point to the underlying byte array.
   *
   * @param buf byte array to point to
   * @param sizeInBytes the number of bytes valid in the byte array
   */
  public void pointTo(byte[] buf, int sizeInBytes) {
    pointTo(buf, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
  }

  public void setTotalSize(int sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  public void setNotNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.unset(baseObject, baseOffset, i);
  }


  public void setNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.set(baseObject, baseOffset, i);
    // To preserve row equality, zero out the value when setting the column to null.
    // Since this row does does not currently support updates to variable-length values, we don't
    // have to worry about zeroing out that data.
    Platform.putLong(baseObject, getFieldOffset(i), 0);
  }

  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putInt(baseObject, getFieldOffset(ordinal), value);
  }

  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putLong(baseObject, getFieldOffset(ordinal), value);
  }

  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    Platform.putDouble(baseObject, getFieldOffset(ordinal), value);
  }

  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putBoolean(baseObject, getFieldOffset(ordinal), value);
  }

  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putShort(baseObject, getFieldOffset(ordinal), value);
  }

  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putByte(baseObject, getFieldOffset(ordinal), value);
  }

  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    Platform.putFloat(baseObject, getFieldOffset(ordinal), value);
  }


  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSetMethods.isSet(baseObject, baseOffset, ordinal);
  }


  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getBoolean(baseObject, getFieldOffset(ordinal));
  }


  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getByte(baseObject, getFieldOffset(ordinal));
  }


  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getShort(baseObject, getFieldOffset(ordinal));
  }


  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getInt(baseObject, getFieldOffset(ordinal));
  }


  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getLong(baseObject, getFieldOffset(ordinal));
  }


  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getFloat(baseObject, getFieldOffset(ordinal));
  }


  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getDouble(baseObject, getFieldOffset(ordinal));
  }


  public UTF8String getUTF8String(int ordinal) {
    if (isNullAt(ordinal)) return null;
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
  }
  public byte[] getBinary(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      final byte[] bytes = new byte[size];
      Platform.copyMemory(
        baseObject,
        baseOffset + offset,
        bytes,
        Platform.BYTE_ARRAY_OFFSET,
        size
      );
      return bytes;
    }
  }



  /**
   * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
   * byte array rather than referencing data stored in a data page.
   */
  public UnsafeRow copy() {
    UnsafeRow rowCopy = new UnsafeRow(numFields);
    final byte[] rowDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
      baseObject,
      baseOffset,
      rowDataCopy,
      Platform.BYTE_ARRAY_OFFSET,
      sizeInBytes
    );
    rowCopy.pointTo(rowDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return rowCopy;
  }

  /**
   * Creates an empty UnsafeRow from a byte array with specified numBytes and numFields.
   * The returned row is invalid until we call copyFrom on it.
   */
  public static UnsafeRow createFromByteArray(int numBytes, int numFields) {
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(new byte[numBytes], numBytes);
    return row;
  }

  /**
   * Copies the input UnsafeRow to this UnsafeRow, and resize the underlying byte[] when the
   * input row is larger than this row.
   */
  public void copyFrom(UnsafeRow row) {
    // copyFrom is only available for UnsafeRow created from byte array.
    assert (baseObject instanceof byte[]) && baseOffset == Platform.BYTE_ARRAY_OFFSET;
    if (row.sizeInBytes > this.sizeInBytes) {
      // resize the underlying byte[] if it's not large enough.
      this.baseObject = new byte[row.sizeInBytes];
    }
    Platform.copyMemory(
      row.baseObject, row.baseOffset, this.baseObject, this.baseOffset, row.sizeInBytes);
    // update the sizeInBytes.
    this.sizeInBytes = row.sizeInBytes;
  }

  /**
   * Write this UnsafeRow's underlying bytes to the given OutputStream.
   *
   * @param out the stream to write to.
   * @param writeBuffer a byte array for buffering chunks of off-heap data while writing to the
   *                    output stream. If this row is backed by an on-heap byte array, then this
   *                    buffer will not be used and may be null.
   */
  public void writeToStream(OutputStream out, byte[] writeBuffer) throws IOException {
    if (baseObject instanceof byte[]) {
      int offsetInByteArray = (int) (Platform.BYTE_ARRAY_OFFSET - baseOffset);
      out.write((byte[]) baseObject, offsetInByteArray, sizeInBytes);
    } else {
      int dataRemaining = sizeInBytes;
      long rowReadPosition = baseOffset;
      while (dataRemaining > 0) {
        int toTransfer = Math.min(writeBuffer.length, dataRemaining);
        Platform.copyMemory(
          baseObject, rowReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
        out.write(writeBuffer, 0, toTransfer);
        rowReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }
    }
  }

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UnsafeRow) {
      UnsafeRow o = (UnsafeRow) other;
      return (sizeInBytes == o.sizeInBytes) &&
        ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
          sizeInBytes);
    }
    return false;
  }

  /**
   * Returns the underlying bytes for this UnsafeRow.
   */
  public byte[] getBytes() {
    if (baseObject instanceof byte[] && baseOffset == Platform.BYTE_ARRAY_OFFSET
      && (((byte[]) baseObject).length == sizeInBytes)) {
      return (byte[]) baseObject;
    } else {
      byte[] bytes = new byte[sizeInBytes];
      Platform.copyMemory(baseObject, baseOffset, bytes, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
      return bytes;
    }
  }

  public static final byte NULL_MARK = (byte) 251;
  public static final byte EMPTY_MARK = (byte) 0;

  @Override
  public ByteBuffer write(ByteBuffer bb, FrontendConnection c,
                          boolean writeSocketIfFull) {
    bb = c.checkWriteBuffer(bb,c.getPacketHeaderSize(),writeSocketIfFull);
    BufferUtil.writeUB3(bb, calcPacketSize());
    bb.put(packetId);
    for (int i = 0; i < numFields; i++) {
      if (!isNullAt(i)) {
        byte[] fv = this.getBinary(i);
        if (fv.length == 0) {
          bb = c.checkWriteBuffer(bb, 1, writeSocketIfFull);
          bb.put(UnsafeRow.EMPTY_MARK);
        } else {
          bb = c.checkWriteBuffer(bb, BufferUtil.getLength(fv),
                  writeSocketIfFull);
          BufferUtil.writeLength(bb, fv.length);
          /**
           * 把数据写到Writer Buffer中
           */
          bb = c.writeToBuffer(fv, bb);
        }
      } else {
        //Col null value
        bb = c.checkWriteBuffer(bb,1,writeSocketIfFull);
        bb.put(UnsafeRow.NULL_MARK);
      }
    }
    return bb;
  }

  @Override
  public int calcPacketSize() {
    int size = 0;
    for (int i = 0; i < numFields; i++) {
      byte[] v = this.getBinary(i);
      size += (v == null || v.length == 0) ? 1 : BufferUtil.getLength(v);
    }
    return size;
  }
  
  	public BigDecimal getDecimal(int ordinal, int scale) {
		if (isNullAt(ordinal)) {
			return null;
		}
		byte[] bytes = getBinary(ordinal);
		BigInteger bigInteger = new BigInteger(bytes);
		BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
		return javaDecimal;
	}
  	
  	/**
 	 * update <strong>exist</strong> decimal column value to new decimal value
 	 * 
 	 * NOTE: decimal max precision is limit to 38
 	 * @param ordinal
 	 * @param value
 	 * @param precision
 	 */
 	public void updateDecimal(int ordinal, BigDecimal value) {
 		assertIndexIsValid(ordinal);
 		// fixed length
 		long cursor = getLong(ordinal) >>> 32;
 		assert cursor > 0 : "invalid cursor " + cursor;
 		// zero-out the bytes
 		Platform.putLong(baseObject, baseOffset + cursor, 0L);
 		Platform.putLong(baseObject, baseOffset + cursor + 8, 0L);
 
 		if (value == null) {
 			setNullAt(ordinal);
 			// keep the offset for future update
 			Platform.putLong(baseObject, getFieldOffset(ordinal), cursor << 32);
 		} else {
 
 			final BigInteger integer = value.unscaledValue();
 			byte[] bytes = integer.toByteArray();
 			assert (bytes.length <= 16);
 
 			// Write the bytes to the variable length portion.
 			Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, baseObject, baseOffset + cursor, bytes.length);
 			setLong(ordinal, (cursor << 32) | ((long) bytes.length));
 		}
 
 	}

  /**
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (isNullAt(ordinal)) {
      return null;
    }
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(ordinal), precision, scale);
    } else {
      byte[] bytes = getBinary(ordinal);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  public void setDecimal(int ordinal, Decimal value, int precision) {
    assertIndexIsValid(ordinal);
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // compact format
      if (value == null) {
        setNullAt(ordinal);
      } else {
        setLong(ordinal, value.toUnscaledLong());
      }
    } else {
      // fixed length
      long cursor = getLong(ordinal) >>> 32;
      assert cursor > 0 : "invalid cursor " + cursor;
      // zero-out the bytes
      Platform.putLong(baseObject, baseOffset + cursor, 0L);
      Platform.putLong(baseObject, baseOffset + cursor + 8, 0L);

      if (value == null) {
        setNullAt(ordinal);
        // keep the offset for future update
        Platform.putLong(baseObject, getFieldOffset(ordinal), cursor << 32);
      } else {

        final BigInteger integer = value.toJavaBigDecimal().unscaledValue();
        byte[] bytes = integer.toByteArray();
        assert(bytes.length <= 16);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
                bytes, Platform.BYTE_ARRAY_OFFSET, baseObject, baseOffset + cursor, bytes.length);
        setLong(ordinal, (cursor << 32) | ((long) bytes.length));
      }
    }
  }

*/
  @Override
  protected String getPacketInfo() {
    return "MySQL RowData Packet";
  }

  // This is for debugging
  @Override
  public String toString() {
    StringBuilder build = new StringBuilder("[");
    for (int i = 0; i < sizeInBytes; i += 8) {
      if (i != 0) build.append(',');
      build.append(Long.toHexString(Platform.getLong(baseObject, baseOffset + i)));
    }
    build.append(']');
    return build.toString();
  }

  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes / 8);
  }

}
