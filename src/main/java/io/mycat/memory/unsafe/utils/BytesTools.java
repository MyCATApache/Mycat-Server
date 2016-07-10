package io.mycat.memory.unsafe.utils;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.common.annotations.VisibleForTesting;

import io.mycat.memory.unsafe.Platform;
import sun.misc.Unsafe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Utility class that handles byte arrays, conversions to/from other types,
 */
@SuppressWarnings("restriction")
public class BytesTools {

    //HConstants.UTF8_ENCODING should be updated if this changed
    /** When we encode strings, we always specify UTF8 encoding */
    private static final String UTF8_ENCODING = "UTF-8";

    //HConstants.UTF8_CHARSET should be updated if this changed
     /** When we encode strings, we always specify UTF8 encoding */
    private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);

    /**
     * Size of boolean in bytes
     */
    public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

    /**
     * Size of byte in bytes
     */
    public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

    /**
     * Size of char in bytes
     */
    public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

    /**
     * Size of double in bytes
     */
    public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

    /**
     * Size of float in bytes
     */
    public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

    /**
     * Size of int in bytes
     */
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    /**
     * Size of long in bytes
     */
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    /**
     * Size of short in bytes
     */
    public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

    /**
     * Mask to apply to a long to reveal the lower int only. Use like this:
     * int i = (int)(0xFFFFFFFF00000000L ^ some_long_value);
     */
    public static final long MASK_FOR_LOWER_INT_IN_LONG = 0xFFFFFFFF00000000L;

    private static final boolean UNSAFE_UNALIGNED = Platform.unaligned();
    /**
     * Returns a new byte array, copied from the given {@code buf},
     * from the index 0 (inclusive) to the limit (exclusive),
     * regardless of the current position.
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return the byte array
     * @see #getBytes(ByteBuffer)
     */
    public static byte[] toBytes(ByteBuffer buf) {
        ByteBuffer dup = buf.duplicate();
        dup.position(0);
        return readBytes(dup);
    }

    private static byte[] readBytes(ByteBuffer buf) {
        byte [] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    /**
     * @param b Presumed UTF-8 encoded byte array.
     * @return String made from <code>b</code>
     */
    public static String toString(final byte [] b) {
        if (b == null) {
            return null;
        }
        return toString(b, 0, b.length);
    }

    /**
     * Joins two byte arrays together using a separator.
     * @param b1 The first byte array.
     * @param sep The separator to use.
     * @param b2 The second byte array.
     */
    public static String toString(final byte [] b1,
                                  String sep,
                                  final byte [] b2) {
        return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
    }

    /**
     * This method will convert utf8 encoded bytes into a string. If
     * the given byte array is null, this method will return null.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @return String made from <code>b</code> or null
     */
    public static String toString(final byte [] b, int off) {
        if (b == null) {
            return null;
        }
        int len = b.length - off;
        if (len <= 0) {
            return "";
        }
        return new String(b, off, len, UTF8_CHARSET);
    }

    /**
     * This method will convert utf8 encoded bytes into a string. If
     * the given byte array is null, this method will return null.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @param len length of utf-8 sequence
     * @return String made from <code>b</code> or null
     */
    public static String toString(final byte [] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, UTF8_CHARSET);
    }

    /**
     * Write a printable representation of a byte array.
     *
     * @param b byte array
     * @return string
     * @see #toStringBinary(byte[], int, int)
     */
    public static String toStringBinary(final byte [] b) {
        if (b == null)
            return "null";
        return toStringBinary(b, 0, b.length);
    }

    /**
     * Converts the given byte buffer to a printable representation,
     * from the index 0 (inclusive) to the limit (exclusive),
     * regardless of the current position.
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return a string representation of the buffer's binary contents
     * @see #toBytes(ByteBuffer)
     * @see #getBytes(ByteBuffer)
     */
    public static String toStringBinary(ByteBuffer buf) {
        if (buf == null)
            return "null";
        if (buf.hasArray()) {
            return toStringBinary(buf.array(), buf.arrayOffset(), buf.limit());
        }
        return toStringBinary(toBytes(buf));
    }

    private static final char[] HEX_CHARS_UPPER = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg:
     * \x00 \x05 etc
     *
     * @param b array to write out
     * @param off offset to start at
     * @param len length to write
     * @return string output
     */
    public static String toStringBinary(final byte [] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        // Just in case we are passed a 'len' that is > buffer length...
        if (off >= b.length) return result.toString();
        if (off + len > b.length) len = b.length - off;
        for (int i = off; i < off + len ; ++i) {
            int ch = b[i] & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\') {
                result.append((char)ch);
            } else {
                result.append("\\x");
                result.append(HEX_CHARS_UPPER[ch / 0x10]);
                result.append(HEX_CHARS_UPPER[ch % 0x10]);
            }
        }
        return result.toString();
    }

    private static boolean isHexDigit(char c) {
        return
                (c >= 'A' && c <= 'F') ||
                        (c >= '0' && c <= '9');
    }

    /**
     * Takes a ASCII digit in the range A-F0-9 and returns
     * the corresponding integer/ordinal value.
     * @param ch  The hex digit.
     * @return The converted hex value as a byte.
     */
    public static byte toBinaryFromHex(byte ch) {
        if (ch >= 'A' && ch <= 'F')
            return (byte) ((byte)10 + (byte) (ch - 'A'));
        // else
        return (byte) (ch - '0');
    }

    public static byte [] toBytesBinary(String in) {
        // this may be bigger than we need, but let's be safe.
        byte [] b = new byte[in.length()];
        int size = 0;
        for (int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i+1 && in.charAt(i+1) == 'x') {
                // ok, take next 2 hex digits.
                char hd1 = in.charAt(i+2);
                char hd2 = in.charAt(i+3);

                // they need to be A-F0-9:
                if (!isHexDigit(hd1) ||
                        !isHexDigit(hd2)) {
                    // bogus escape code, ignore:
                    continue;
                }
                // turn hex ASCII digit -> number
                byte d = (byte) ((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));

                b[size++] = d;
                i += 3; // skip 3
            } else {
                b[size++] = (byte) ch;
            }
        }
        // resize:
        byte [] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }

    /**
     * Converts a string to a UTF-8 byte array.
     * @param s string
     * @return the byte array
     */
    public static byte[] toBytes(String s) {
        return s.getBytes(UTF8_CHARSET);
    }

    /**
     * Convert a boolean to a byte array. True becomes -1
     * and false becomes 0.
     *
     * @param b value
     * @return <code>b</code> encoded in a byte array.
     */
    public static byte [] toBytes(final boolean b) {
        return new byte[] { b ? (byte) -1 : (byte) 0 };
    }

    /**
     * Reverses {@link #toBytes(boolean)}
     * @param b array
     * @return True or false.
     */
    public static boolean toBoolean(final byte [] b) {
        if (b.length != 1) {
            throw new IllegalArgumentException("Array has wrong size: " + b.length);
        }
        return b[0] != (byte) 0;
    }

    /**
     * Convert a long value to a byte array using big-endian.
     *
     * @param val value to convert
     * @return the byte array
     */
    public static byte[] toBytes(long val) {
        byte [] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /**
     * Converts a byte array to a long value. Reverses
     * {@link #toBytes(long)}
     * @param bytes array
     * @return the long value
     */
    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value. Assumes there will be
     * {@link #SIZEOF_LONG} bytes available.
     *
     * @param bytes bytes
     * @param offset offset
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset) {
        return toLong(bytes, offset, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value.
     *
     * @param bytes array of bytes
     * @param offset offset into array
     * @param length length of data (must be {@link #SIZEOF_LONG})
     * @return the long value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_LONG} or
     * if there's not enough room in the array at the offset indicated.
     */
    public static long toLong(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_LONG || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
        }
        if (UNSAFE_UNALIGNED) {
            return Platform.toLong(bytes, offset);
        } else {
            long l = 0;
            for(int i = offset; i < offset + length; i++) {
                l <<= 8;
                l ^= bytes[i] & 0xFF;
            }
            return l;
        }
    }

    private static IllegalArgumentException
    explainWrongLengthOrOffset(final byte[] bytes,
                               final int offset,
                               final int length,
                               final int expectedLength) {
        String reason;
        if (length != expectedLength) {
            reason = "Wrong length: " + length + ", expected " + expectedLength;
        } else {
            reason = "offset (" + offset + ") + length (" + length + ") exceed the"
                    + " capacity of the array: " + bytes.length;
        }
        return new IllegalArgumentException(reason);
    }

    /**
     * Put a long value out to the specified byte array position.
     * @param bytes the byte array
     * @param offset position in the array
     * @param val long to write out
     * @return incremented offset
     * @throws IllegalArgumentException if the byte array given doesn't have
     * enough room at the offset specified.
     */
    public static int putLong(byte[] bytes, int offset, long val) {
        if (bytes.length - offset < SIZEOF_LONG) {
            throw new IllegalArgumentException("Not enough room to put a long at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        if (UNSAFE_UNALIGNED) {
            return Platform.putLong(bytes, offset, val);
        } else {
            for(int i = offset + 7; i > offset; i--) {
                bytes[i] = (byte) val;
                val >>>= 8;
            }
            bytes[offset] = (byte) val;
            return offset + SIZEOF_LONG;
        }
    }

    /**
     * Put a long value out to the specified byte array position (Unsafe).
     * @param bytes the byte array
     * @param offset position in the array
     * @param val long to write out
     * @return incremented offset
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     */
    @Deprecated
    public static int putLongUnsafe(byte[] bytes, int offset, long val) {
        return Platform.putLong(bytes, offset, val);
    }

    /**
     * Presumes float encoded as IEEE 754 floating-point "single format"
     * @param bytes byte array
     * @return Float made from passed byte array.
     */
    public static float toFloat(byte [] bytes) {
        return toFloat(bytes, 0);
    }

    /**
     * Presumes float encoded as IEEE 754 floating-point "single format"
     * @param bytes array to convert
     * @param offset offset into array
     * @return Float made from passed byte array.
     */
    public static float toFloat(byte [] bytes, int offset) {
        return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
    }

    /**
     * @param bytes byte array
     * @param offset offset to write to
     * @param f float value
     * @return New offset in <code>bytes</code>
     */
    public static int putFloat(byte [] bytes, int offset, float f) {
        return putInt(bytes, offset, Float.floatToRawIntBits(f));
    }

    /**
     * @param f float value
     * @return the float represented as byte []
     */
    public static byte [] toBytes(final float f) {
        // Encode it as int
        return BytesTools.toBytes(Float.floatToRawIntBits(f));
    }

    /**
     * @param bytes byte array
     * @return Return double made from passed bytes.
     */
    public static double toDouble(final byte [] bytes) {
        return toDouble(bytes, 0);
    }

    /**
     * @param bytes byte array
     * @param offset offset where double is
     * @return Return double made from passed bytes.
     */
    public static double toDouble(final byte [] bytes, final int offset) {
        return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
    }

    /**
     * @param bytes byte array
     * @param offset offset to write to
     * @param d value
     * @return New offset into array <code>bytes</code>
     */
    public static int putDouble(byte [] bytes, int offset, double d) {
        return putLong(bytes, offset, Double.doubleToLongBits(d));
    }

    /**
     * Serialize a double as the IEEE 754 double format output. The resultant
     * array will be 8 bytes long.
     *
     * @param d value
     * @return the double represented as byte []
     */
    public static byte [] toBytes(final double d) {
        // Encode it as a long
        return BytesTools.toBytes(Double.doubleToRawLongBits(d));
    }

    /**
     * Convert an int value to a byte array.  Big-endian.  Same as what DataOutputStream.writeInt
     * does.
     *
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(int val) {
        byte [] b = new byte[4];
        for(int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @return the int value
     */
    public static int toInt(byte[] bytes) {
        return toInt(bytes, 0, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @param offset offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
        return toInt(bytes, offset, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @param offset offset into array
     * @param length length of int (has to be {@link #SIZEOF_INT})
     * @return the int value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
     * if there's not enough room in the array at the offset indicated.
     */
    public static int toInt(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_INT || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
        }
        if (UNSAFE_UNALIGNED) {
            return Platform.toInt(bytes, offset);
        } else {
            int n = 0;
            for(int i = offset; i < (offset + length); i++) {
                n <<= 8;
                n ^= bytes[i] & 0xFF;
            }
            return n;
        }
    }

    /**
     * Converts a byte array to an int value (Unsafe version)
     * @param bytes byte array
     * @param offset offset into array
     * @return the int value
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     */
    @Deprecated
    public static int toIntUnsafe(byte[] bytes, int offset) {
        return Platform.toInt(bytes, offset);
    }

    /**
     * Converts a byte array to an short value (Unsafe version)
     * @param bytes byte array
     * @param offset offset into array
     * @return the short value
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     */
    @Deprecated
    public static short toShortUnsafe(byte[] bytes, int offset) {
        return Platform.toShort(bytes, offset);
    }

    /**
     * Converts a byte array to an long value (Unsafe version)
     * @param bytes byte array
     * @param offset offset into array
     * @return the long value
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     */
    @Deprecated
    public static long toLongUnsafe(byte[] bytes, int offset) {
        return Platform.toLong(bytes, offset);
    }

    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @param offset offset into array
     * @param length how many bytes should be considered for creating int
     * @return the int value
     * @throws IllegalArgumentException if there's not enough room in the array at the offset
     * indicated.
     */
    public static int readAsInt(byte[] bytes, int offset, final int length) {
        if (offset + length > bytes.length) {
            throw new IllegalArgumentException("offset (" + offset + ") + length (" + length
                    + ") exceed the" + " capacity of the array: " + bytes.length);
        }
        int n = 0;
        for(int i = offset; i < (offset + length); i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    /**
     * Put an int value out to the specified byte array position.
     * @param bytes the byte array
     * @param offset position in the array
     * @param val int to write out
     * @return incremented offset
     * @throws IllegalArgumentException if the byte array given doesn't have
     * enough room at the offset specified.
     */
    public static int putInt(byte[] bytes, int offset, int val) {
        if (bytes.length - offset < SIZEOF_INT) {
            throw new IllegalArgumentException("Not enough room to put an int at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        if (UNSAFE_UNALIGNED) {
            return Platform.putInt(bytes, offset, val);
        } else {
            for(int i= offset + 3; i > offset; i--) {
                bytes[i] = (byte) val;
                val >>>= 8;
            }
            bytes[offset] = (byte) val;
            return offset + SIZEOF_INT;
        }
    }

    /**
     * Put an int value out to the specified byte array position (Unsafe).
     * @param bytes the byte array
     * @param offset position in the array
     * @param val int to write out
     * @return incremented offset
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     */
    @Deprecated
    public static int putIntUnsafe(byte[] bytes, int offset, int val) {
        return Platform.putInt(bytes, offset, val);
    }

    /**
     * Convert a short value to a byte array of {@link #SIZEOF_SHORT} bytes long.
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(short val) {
        byte[] b = new byte[SIZEOF_SHORT];
        b[1] = (byte) val;
        val >>= 8;
        b[0] = (byte) val;
        return b;
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @return the short value
     */
    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0, SIZEOF_SHORT);
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @param offset offset into array
     * @return the short value
     */
    public static short toShort(byte[] bytes, int offset) {
        return toShort(bytes, offset, SIZEOF_SHORT);
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @param offset offset into array
     * @param length length, has to be {@link #SIZEOF_SHORT}
     * @return the short value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_SHORT}
     * or if there's not enough room in the array at the offset indicated.
     */
    public static short toShort(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_SHORT || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
        }
        if (UNSAFE_UNALIGNED) {
            return Platform.toShort(bytes, offset);
        } else {
            short n = 0;
            n ^= bytes[offset] & 0xFF;
            n <<= 8;
            n ^= bytes[offset+1] & 0xFF;
            return n;
        }
    }

    /**
     * Returns a new byte array, copied from the given {@code buf},
     * from the position (inclusive) to the limit (exclusive).
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return the byte array
     * @see #toBytes(ByteBuffer)
     */
    public static byte[] getBytes(ByteBuffer buf) {
        return readBytes(buf.duplicate());
    }

    /**
     * Put a short value out to the specified byte array position.
     * @param bytes the byte array
     * @param offset position in the array
     * @param val short to write out
     * @return incremented offset
     * @throws IllegalArgumentException if the byte array given doesn't have
     * enough room at the offset specified.
     */
    public static int putShort(byte[] bytes, int offset, short val) {
        if (bytes.length - offset < SIZEOF_SHORT) {
            throw new IllegalArgumentException("Not enough room to put a short at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        if (UNSAFE_UNALIGNED) {
            return Platform.putShort(bytes, offset, val);
        } else {
            bytes[offset+1] = (byte) val;
            val >>= 8;
            bytes[offset] = (byte) val;
            return offset + SIZEOF_SHORT;
        }
    }

    /**
     * Put a short value out to the specified byte array position (Unsafe).
     * @param bytes the byte array
     * @param offset position in the array
     * @param val short to write out
     * @return incremented offset
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
     */
    @Deprecated
    public static int putShortUnsafe(byte[] bytes, int offset, short val) {
        return Platform.putShort(bytes, offset, val);
    }

    /**
     * Put an int value as short out to the specified byte array position. Only the lower 2 bytes of
     * the short will be put into the array. The caller of the API need to make sure they will not
     * loose the value by doing so. This is useful to store an unsigned short which is represented as
     * int in other parts.
     * @param bytes the byte array
     * @param offset position in the array
     * @param val value to write out
     * @return incremented offset
     * @throws IllegalArgumentException if the byte array given doesn't have
     * enough room at the offset specified.
     */
    public static int putAsShort(byte[] bytes, int offset, int val) {
        if (bytes.length - offset < SIZEOF_SHORT) {
            throw new IllegalArgumentException("Not enough room to put a short at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        bytes[offset+1] = (byte) val;
        val >>= 8;
        bytes[offset] = (byte) val;
        return offset + SIZEOF_SHORT;
    }

    /**
     * Convert a BigDecimal value to a byte array
     *
     * @param val
     * @return the byte array
     */
    public static byte[] toBytes(BigDecimal val) {
        byte[] valueBytes = val.unscaledValue().toByteArray();
        byte[] result = new byte[valueBytes.length + SIZEOF_INT];
        int offset = putInt(result, 0, val.scale());
        putBytes(result, offset, valueBytes, 0, valueBytes.length);
        return result;
    }

    /**
     * Put bytes at the specified byte array position.
     * @param tgtBytes the byte array
     * @param tgtOffset position in the array
     * @param srcBytes array to write out
     * @param srcOffset source offset
     * @param srcLength source length
     * @return incremented offset
     * */

  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes, int srcOffset, int srcLength) {
      System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
      return tgtOffset + srcLength;
  }

    /**
     * Converts a byte array to a BigDecimal
     *
     * @param bytes
     * @return the char value
     */
    public static BigDecimal toBigDecimal(byte[] bytes) {
        return toBigDecimal(bytes, 0, bytes.length);
    }

    /**
     * Converts a byte array to a BigDecimal value
     *
     * @param bytes
     * @param offset
     * @param length
     * @return the char value
     */
    public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
        if (bytes == null || length < SIZEOF_INT + 1 ||
                (offset + length > bytes.length)) {
            return null;
        }

        int scale = toInt(bytes, offset);
        byte[] tcBytes = new byte[length - SIZEOF_INT];
        System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
        return new BigDecimal(new BigInteger(tcBytes), scale);
    }


    /**
     * @param left left operand
     * @param right right operand
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static int compareTo(final byte [] left, final byte [] right) {
        return LexicographicalComparerHolder.BEST_COMPARER.
                compareTo(left, 0, left.length, right, 0, right.length);
    }

    /**
     * Lexicographically compare two arrays.
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @param offset1 Where to start comparing in the left buffer
     * @param offset2 Where to start comparing in the right buffer
     * @param length1 How much to compare from the left buffer
     * @param length2 How much to compare from the right buffer
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static int compareTo(byte[] buffer1, int offset1, int length1,
                                byte[] buffer2, int offset2, int length2) {
        return LexicographicalComparerHolder.BEST_COMPARER.
                compareTo(buffer1, offset1, length1, buffer2, offset2, length2);
    }

    interface Comparer<T> {
        int compareTo(
                T buffer1, int offset1, int length1, T buffer2, int offset2, int length2
        );
    }

    @VisibleForTesting
    static Comparer<byte[]> lexicographicalComparerJavaImpl() {
        return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
    }

    /**
     * Provides a lexicographical comparer implementation; either a Java
     * implementation or a faster implementation based on {@link Unsafe}.
     *
     * <p>Uses reflection to gracefully fall back to the Java implementation if
     * {@code Unsafe} isn't available.
     */
    @VisibleForTesting
    static class LexicographicalComparerHolder {
        static final String UNSAFE_COMPARER_NAME =
                LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";

        static final Comparer<byte[]> BEST_COMPARER = getBestComparer();
        /**
         * Returns the Unsafe-using Comparer, or falls back to the pure-Java
         * implementation if unable to do so.
         */
        static Comparer<byte[]> getBestComparer() {
            try {
                Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

                // yes, UnsafeComparer does implement Comparer<byte[]>
                @SuppressWarnings("unchecked")
                Comparer<byte[]> comparer =
                        (Comparer<byte[]>) theClass.getEnumConstants()[0];
                return comparer;
            } catch (Throwable t) { // ensure we really catch *everything*
                return lexicographicalComparerJavaImpl();
            }
        }

        enum PureJavaComparer implements Comparer<byte[]> {
            INSTANCE;

            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1,
                                 byte[] buffer2, int offset2, int length2) {
                // Short circuit equal case
                if (buffer1 == buffer2 &&
                        offset1 == offset2 &&
                        length1 == length2) {
                    return 0;
                }
                // Bring WritableComparator code local
                int end1 = offset1 + length1;
                int end2 = offset2 + length2;
                for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                    int a = (buffer1[i] & 0xff);
                    int b = (buffer2[j] & 0xff);
                    if (a != b) {
                        return a - b;
                    }
                }
                return length1 - length2;
            }
        }
    }

    /**
     * @param left left operand
     * @param right right operand
     * @return True if equal
     */
    public static boolean equals(final byte [] left, final byte [] right) {
        // Could use Arrays.equals?
        //noinspection SimplifiableConditionalExpression
        if (left == right) return true;
        if (left == null || right == null) return false;
        if (left.length != right.length) return false;
        if (left.length == 0) return true;

        // Since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (left[left.length - 1] != right[right.length - 1]) return false;

        return compareTo(left, right) == 0;
    }

    public static boolean equals(final byte[] left, int leftOffset, int leftLen,
                                 final byte[] right, int rightOffset, int rightLen) {
        // short circuit case
        if (left == right &&
                leftOffset == rightOffset &&
                leftLen == rightLen) {
            return true;
        }
        // different lengths fast check
        if (leftLen != rightLen) {
            return false;
        }
        if (leftLen == 0) {
            return true;
        }

        // Since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (left[leftOffset + leftLen - 1] != right[rightOffset + rightLen - 1]) return false;

        return LexicographicalComparerHolder.BEST_COMPARER.
                compareTo(left, leftOffset, leftLen, right, rightOffset, rightLen) == 0;
    }


    /**
     * @param a left operand
     * @param buf right operand
     * @return True if equal
     */
    public static boolean equals(byte[] a, ByteBuffer buf) {
        if (a == null) return buf == null;
        if (buf == null) return false;
        if (a.length != buf.remaining()) return false;

        // Thou shalt not modify the original byte buffer in what should be read only operations.
        ByteBuffer b = buf.duplicate();
        for (byte anA : a) {
            if (anA != b.get()) {
                return false;
            }
        }
        return true;
    }


    /**
     * Return true if the byte array on the right is a prefix of the byte
     * array on the left.
     */
    public static boolean startsWith(byte[] bytes, byte[] prefix) {
        return bytes != null && prefix != null &&
                bytes.length >= prefix.length &&
                LexicographicalComparerHolder.BEST_COMPARER.
                        compareTo(bytes, 0, prefix.length, prefix, 0, prefix.length) == 0;
    }


    /**
     * @param a first third
     * @param b second third
     * @param c third third
     * @return New array made from a, b and c
     */
    public static byte [] add(final byte [] a, final byte [] b, final byte [] c) {
        byte [] result = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        return result;
    }

    /**
     * @param arrays all the arrays to concatenate together.
     * @return New array made from the concatenation of the given arrays.
     */
    public static byte [] add(final byte [][] arrays) {
        int length = 0;
        for (int i = 0; i < arrays.length; i++) {
            length += arrays[i].length;
        }
        byte [] result = new byte[length];
        int index = 0;
        for (int i = 0; i < arrays.length; i++) {
            System.arraycopy(arrays[i], 0, result, index, arrays[i].length);
            index += arrays[i].length;
        }
        return result;
    }

    /**
     * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
     * Useful splitting ranges for MapReduce jobs.
     * @param a Beginning of range
     * @param b End of range
     * @param num Number of times to split range.  Pass 1 if you want to split
     * the range in two; i.e. one split.
     * @return Array of dividing values
     */



    /**
     * @param t operands
     * @return Array of byte arrays made from passed array of Text
     */
    public static byte [][] toByteArrays(final String [] t) {
        byte [][] result = new byte[t.length][];
        for (int i = 0; i < t.length; i++) {
            result[i] = BytesTools.toBytes(t[i]);
        }
        return result;
    }

    /**
     * @param t operands
     * @return Array of binary byte arrays made from passed array of binary strings
     */
    public static byte[][] toBinaryByteArrays(final String[] t) {
        byte[][] result = new byte[t.length][];
        for (int i = 0; i < t.length; i++) {
            result[i] = BytesTools.toBytesBinary(t[i]);
        }
        return result;
    }

    /**
     * @param column operand
     * @return A byte array of a byte array where first and only entry is
     * <code>column</code>
     */
    public static byte [][] toByteArrays(final String column) {
        return toByteArrays(toBytes(column));
    }

    /**
     * @param column operand
     * @return A byte array of a byte array where first and only entry is
     * <code>column</code>
     */
    public static byte [][] toByteArrays(final byte [] column) {
        byte [][] result = new byte[1][];
        result[0] = column;
        return result;
    }
}