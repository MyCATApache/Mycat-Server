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

import java.io.UnsupportedEncodingException;
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
     * Convert a byte array  to a int value
     * @param buf
     * @return int
     * @throws NumberFormatException
     */

    public static int getInt(byte[] buf) throws NumberFormatException {
        return getInt(buf, 0, buf.length);
    }

    public static int getInt(byte[] buf, int offset, int endPos) throws NumberFormatException {
        byte base = 10;

        int s;
        for(s = offset; s < endPos && Character.isWhitespace((char)buf[s]); ++s) {
            ;
        }
        if(s == endPos) {
            throw new NumberFormatException(toString(buf));
        } else {
            boolean negative = false;
            if((char)buf[s] == 45) {
                negative = true;
                ++s;
            } else if((char)buf[s] == 43) {
                ++s;
            }

            int save = s;
            int cutoff = 2147483647 / base;
            int cutlim = 2147483647 % base;
            if(negative) {
                ++cutlim;
            }

            boolean overflow = false;

            int i;
            for(i = 0; s < endPos; ++s) {
                char c = (char)buf[s];
                if(Character.isDigit(c)) {
                    c = (char)(c - 48);
                } else {
                    if(!Character.isLetter(c)) {
                        break;
                    }

                    c = (char)(Character.toUpperCase(c) - 65 + 10);
                }

                if(c >= base) {
                    break;
                }

                if(i <= cutoff && (i != cutoff || c <= cutlim)) {
                    i *= base;
                    i += c;
                } else {
                    overflow = true;
                }
            }

            if(s == save) {
                throw new NumberFormatException(toString(buf));
            } else if(overflow) {
                throw new NumberFormatException(toString(buf));
            } else {
                return negative?-i:i;
            }
        }
    }

    /**
     * Convert a byte array to a long value
     * @param buf
     * @return
     * @throws NumberFormatException
     */
    public static long getLong(byte[] buf) throws NumberFormatException {
        return getLong(buf, 0, buf.length);
    }

    public static long getLong(byte[] buf, int offset, int endpos) throws NumberFormatException {
        byte base = 10;

        int s;
        for(s = offset; s < endpos && Character.isWhitespace((char)buf[s]); ++s) {
            ;
        }

        if(s == endpos) {
            throw new NumberFormatException(toString(buf));
        } else {
            boolean negative = false;
            if((char)buf[s] == 45) {
                negative = true;
                ++s;
            } else if((char)buf[s] == 43) {
                ++s;
            }

            int save = s;
            long cutoff = 9223372036854775807L / (long)base;
            long cutlim = (long)((int)(9223372036854775807L % (long)base));
            if(negative) {
                ++cutlim;
            }

            boolean overflow = false;

            long i;
            for(i = 0L; s < endpos; ++s) {
                char c = (char)buf[s];
                if(Character.isDigit(c)) {
                    c = (char)(c - 48);
                } else {
                    if(!Character.isLetter(c)) {
                        break;
                    }
                    c = (char)(Character.toUpperCase(c) - 65 + 10);
                }

                if(c >= base) {
                    break;
                }

                if(i <= cutoff && (i != cutoff || (long)c <= cutlim)) {
                    i *= (long)base;
                    i += (long)c;
                } else {
                    overflow = true;
                }
            }

            if(s == save) {
                throw new NumberFormatException(toString(buf));
            } else if(overflow) {
                throw new NumberFormatException(toString(buf));
            } else {
                return negative?-i:i;
            }
        }
    }

    /**
     * Convert a byte array  to a short value
     * @param buf
     * @return
     * @throws NumberFormatException
     */
    public static short getShort(byte[] buf) throws NumberFormatException {
        return getShort(buf, 0, buf.length);
    }

    public static short getShort(byte[] buf, int offset, int endpos) throws NumberFormatException {
        byte base = 10;

        int s;
        for(s = offset; s < endpos && Character.isWhitespace((char)buf[s]); ++s) {
            ;
        }

        if(s == endpos) {
            throw new NumberFormatException(toString(buf));
        } else {
            boolean negative = false;
            if((char)buf[s] == 45) {
                negative = true;
                ++s;
            } else if((char)buf[s] == 43) {
                ++s;
            }

            int save = s;
            short cutoff = (short)(32767 / base);
            short cutlim = (short)(32767 % base);
            if(negative) {
                ++cutlim;
            }

            boolean overflow = false;

            short i;
            for(i = 0; s < endpos; ++s) {
                char c = (char)buf[s];
                if(Character.isDigit(c)) {
                    c = (char)(c - 48);
                } else {
                    if(!Character.isLetter(c)) {
                        break;
                    }

                    c = (char)(Character.toUpperCase(c) - 65 + 10);
                }

                if(c >= base) {
                    break;
                }

                if(i <= cutoff && (i != cutoff || c <= cutlim)) {
                    i = (short)(i * base);
                    i = (short)(i + c);
                } else {
                    overflow = true;
                }
            }

            if(s == save) {
                throw new NumberFormatException(toString(buf));
            } else if(overflow) {
                throw new NumberFormatException(toString(buf));
            } else {
                return negative?(short)(-i):i;
            }
        }
    }

    /**
     *  Convert a byte array  to a float value
     * @param src
     * @return
     * @throws UnsupportedEncodingException
     */
    public static float getFloat(byte [] src) throws UnsupportedEncodingException {
        return Float.parseFloat(new String(src,"US-ASCII"));
    }

    /**
     * Convert a byte array  to a double value
     * @param src
     * @return
     * @throws UnsupportedEncodingException
     */

    public static double getDouble(byte [] src) throws UnsupportedEncodingException {
        return  Double.parseDouble(new String(src,"US-ASCII"));
    }

    /**
     * Convert a long value to a byte array
     * @param l
     * @return
     * @throws UnsupportedEncodingException
     */


    public static byte[] long2Bytes(long l) throws UnsupportedEncodingException {
        String lstr = Long.toString(l);
        return lstr.getBytes("US-ASCII");
    }

    /**
     * Convert a int value to a byte array
     * @param i
     * @return
     * @throws UnsupportedEncodingException
     */

    public static byte[] int2Bytes(int i) throws UnsupportedEncodingException {
        String istr = Integer.toString(i);
        return istr.getBytes("US-ASCII");
    }

    /**
     * Convert a short value to a byte array
     * @param i
     * @return
     * @throws UnsupportedEncodingException
     */

    public static byte[] short2Bytes(short i) throws UnsupportedEncodingException {
        String sstr = Short.toString(i);
        return sstr.getBytes("US-ASCII");
    }

    /**
     * Convert a float value to a byte array
     * @param f
     * @return
     * @throws UnsupportedEncodingException
     */
    public static byte[] float2Bytes(float f) throws UnsupportedEncodingException {
        String fstr = Float.toString(f);
        return fstr.getBytes("US-ASCII");
    }

    /**
     * Convert a double value to a byte array
     * @param d
     * @return
     * @throws UnsupportedEncodingException
     */
    public static byte[] double2Bytes(double d) throws UnsupportedEncodingException {
        String dstr = Double.toString(d);
        return dstr.getBytes("US-ASCII");
    }

    /**
     * Returns a new byte array, copied from the given {@code buf},
     * from the index 0 (inclusive) to the limit (exclusive),
     * regardless of the current position.
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return the byte array
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


    public static byte [] paddingInt(byte [] a){

        if(a == null){
            return null;
        }

        if (a.length==SIZEOF_INT){
            return  a;
        }

        byte [] b = new byte[SIZEOF_INT];
        if (Platform.littleEndian){
            for (int i = 0; i < SIZEOF_INT-a.length; i++) {
                b[i] = 0x00;
            }
            System.arraycopy(a, 0, b,SIZEOF_INT-a.length, a.length);
        }else {
            System.arraycopy(a, 0, b, 0, a.length);
            for (int i = a.length; i < SIZEOF_INT; i++) {
                b[i] = 0x00;
            }
        }
        return  b;
    }

    public static byte [] paddingLong(byte [] a){
        if(a == null){
            return null;
        }

        if (a.length==SIZEOF_LONG){
            return  a;
        }

        byte [] b = new byte[SIZEOF_LONG];
        if (Platform.littleEndian){
            for (int i = 0; i < SIZEOF_LONG-a.length; i++) {
                b[i] = 0x00;
            }
            System.arraycopy(a, 0, b,SIZEOF_LONG-a.length, a.length);
        }else {
            System.arraycopy(a, 0, b, 0, a.length);
            for (int i = a.length; i < SIZEOF_LONG; i++) {
                b[i] = 0x00;
            }
        }
        return b;
    }

    public static byte [] paddingShort(byte [] a){

        if(a == null){
            return null;
        }

        if (a.length==SIZEOF_SHORT){
            return  a;
        }
        byte [] b = new byte[SIZEOF_SHORT];
        if (Platform.littleEndian){
            for (int i = 0; i < SIZEOF_SHORT-a.length; i++) {
                b[i] = 0x00;
            }
            System.arraycopy(a, 0, b, SIZEOF_SHORT-a.length, a.length);
        }else {
            System.arraycopy(a, 0, b, 0, a.length);
            for (int i = a.length; i < SIZEOF_SHORT; i++) {
                b[i] = 0x00;
            }
        }
        return b;
    }
}