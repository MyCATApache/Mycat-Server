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

package io.mycat.memory.unsafe;

import io.mycat.memory.unsafe.utils.BytesTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Platform {

    private final static Logger logger = LoggerFactory.getLogger(Platform.class);
    private static final Pattern MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN =
            Pattern.compile("\\s*-XX:MaxDirectMemorySize\\s*=\\s*([0-9]+)\\s*([kKmMgG]?)\\s*$");
    private static final Unsafe _UNSAFE;

    public static final int BYTE_ARRAY_OFFSET;

    public static final int SHORT_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET;

    public static final int DOUBLE_ARRAY_OFFSET;

    private static final long MAX_DIRECT_MEMORY;

    private static final boolean unaligned;

    public static final boolean littleEndian = ByteOrder.nativeOrder()
            .equals(ByteOrder.LITTLE_ENDIAN);

    static {
        boolean _unaligned;
        // use reflection to access unaligned field
        try {
            Class<?> bitsClass =
                    Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
            Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
            unalignedMethod.setAccessible(true);
            _unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
        } catch (Throwable t) {
            // We at least know x86 and x64 support unaligned access.
            String arch = System.getProperty("os.arch", "");
            //noinspection DynamicRegexReplaceableByCompiledPattern
            _unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64)$");
        }
        unaligned = _unaligned;
        MAX_DIRECT_MEMORY = maxDirectMemory();

    }


    private static ClassLoader getSystemClassLoader() {
        return System.getSecurityManager() == null ? ClassLoader.getSystemClassLoader() : (ClassLoader) AccessController.doPrivileged(new PrivilegedAction() {
            public ClassLoader run() {
                return ClassLoader.getSystemClassLoader();
            }
        });
    }

    /**
     * GET  MaxDirectMemory Size,from Netty Project!
     */
    private static long maxDirectMemory() {
        long maxDirectMemory = 0L;
        Class t;
        try {
            t = Class.forName("sun.misc.VM", true, getSystemClassLoader());
            Method runtimeClass = t.getDeclaredMethod("maxDirectMemory", new Class[0]);
            maxDirectMemory = ((Number) runtimeClass.invoke((Object) null, new Object[0])).longValue();
        } catch (Throwable var8) {
            ;
        }

        if (maxDirectMemory > 0L) {
            return maxDirectMemory;
        } else {
            try {
                t = Class.forName("java.lang.management.ManagementFactory", true, getSystemClassLoader());
                Class var10 = Class.forName("java.lang.management.RuntimeMXBean", true, getSystemClassLoader());
                Object runtime = t.getDeclaredMethod("getRuntimeMXBean", new Class[0]).invoke((Object) null, new Object[0]);
                List vmArgs = (List) var10.getDeclaredMethod("getInputArguments", new Class[0]).invoke(runtime, new Object[0]);

                label41:
                for (int i = vmArgs.size() - 1; i >= 0; --i) {
                    Matcher m = MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN.matcher((CharSequence) vmArgs.get(i));
                    if (m.matches()) {
                        maxDirectMemory = Long.parseLong(m.group(1));
                        switch (m.group(2).charAt(0)) {
                            case 'G':
                            case 'g':
                                maxDirectMemory *= 1073741824L;
                                break label41;
                            case 'K':
                            case 'k':
                                maxDirectMemory *= 1024L;
                                break label41;
                            case 'M':
                            case 'm':
                                maxDirectMemory *= 1048576L;
                            default:
                                break label41;
                        }
                    }
                }
            } catch (Throwable var9) {
                logger.error(var9.getMessage());
            }

            if (maxDirectMemory <= 0L) {
                maxDirectMemory = Runtime.getRuntime().maxMemory();
                //System.out.println("maxDirectMemory: {} bytes (maybe)" + Long.valueOf(maxDirectMemory));
            } else {
                //System.out.println("maxDirectMemory: {} bytes" + Long.valueOf(maxDirectMemory));
            }
            return maxDirectMemory;
        }
    }

    public static long getMaxDirectMemory() {
        return MAX_DIRECT_MEMORY;
    }

    public static long getMaxHeapMemory() {
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * @return true when running JVM is having sun's Unsafe package available in it and underlying
     * system having unaligned-access capability.
     */
    public static boolean unaligned() {
        return unaligned;
    }

    public static int getInt(Object object, long offset) {
        return _UNSAFE.getInt(object, offset);
    }

    public static void putInt(Object object, long offset, int value) {
        _UNSAFE.putInt(object, offset, value);
    }

    public static boolean getBoolean(Object object, long offset) {
        return _UNSAFE.getBoolean(object, offset);
    }

    public static void putBoolean(Object object, long offset, boolean value) {
        _UNSAFE.putBoolean(object, offset, value);
    }

    public static byte getByte(Object object, long offset) {
        return _UNSAFE.getByte(object, offset);
    }

    public static void putByte(Object object, long offset, byte value) {
        _UNSAFE.putByte(object, offset, value);
    }

    public static short getShort(Object object, long offset) {
        return _UNSAFE.getShort(object, offset);
    }

    public static void putShort(Object object, long offset, short value) {
        _UNSAFE.putShort(object, offset, value);
    }

    public static long getLong(Object object, long offset) {
        return _UNSAFE.getLong(object, offset);
    }

    public static void putLong(Object object, long offset, long value) {
        _UNSAFE.putLong(object, offset, value);
    }

    public static float getFloat(Object object, long offset) {
        return _UNSAFE.getFloat(object, offset);
    }

    public static void putFloat(Object object, long offset, float value) {
        _UNSAFE.putFloat(object, offset, value);
    }

    public static double getDouble(Object object, long offset) {
        return _UNSAFE.getDouble(object, offset);
    }

    public static void putDouble(Object object, long offset, double value) {
        _UNSAFE.putDouble(object, offset, value);
    }


    public static Object getObjectVolatile(Object object, long offset) {
        return _UNSAFE.getObjectVolatile(object, offset);
    }

    public static void putObjectVolatile(Object object, long offset, Object value) {
        _UNSAFE.putObjectVolatile(object, offset, value);
    }

    public static long allocateMemory(long size) {
        return _UNSAFE.allocateMemory(size);
    }

    public static void freeMemory(long address) {
        _UNSAFE.freeMemory(address);
    }

    public static long reallocateMemory(long address, long oldSize, long newSize) {
        long newMemory = _UNSAFE.allocateMemory(newSize);
        copyMemory(null, address, null, newMemory, oldSize);
        freeMemory(address);
        return newMemory;
    }

    /**
     * Uses internal JDK APIs to allocate a DirectByteBuffer while ignoring the JVM's
     * MaxDirectMemorySize limit (the default limit is too low and we do not want to require users
     * to increase it).
     */
    @SuppressWarnings("unchecked")
    public static ByteBuffer allocateDirectBuffer(int size) {
        try {
            Class cls = Class.forName("java.nio.DirectByteBuffer");
            Constructor constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
            constructor.setAccessible(true);
            Field cleanerField = cls.getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            final long memory = allocateMemory(size);
            ByteBuffer buffer = (ByteBuffer) constructor.newInstance(memory, size);
            Cleaner cleaner = Cleaner.create(buffer, new Runnable() {
                @Override
                public void run() {
                    freeMemory(memory);
                }
            });
            cleanerField.set(buffer, cleaner);
            return buffer;
        } catch (Exception e) {
            throwException(e);
        }
        throw new IllegalStateException("unreachable");
    }

    public static void setMemory(long address, byte value, long size) {
        _UNSAFE.setMemory(address, size, value);
    }

    public static void copyMemory(
            Object src, long srcOffset, Object dst, long dstOffset, long length) {
        // Check if dstOffset is before or after srcOffset to determine if we should copy
        // forward or backwards. This is necessary in case src and dst overlap.
        if (dstOffset < srcOffset) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
            }

        }
    }

    /**
     * Raises an exception bypassing compiler checks for checked exceptions.
     */
    public static void throwException(Throwable t) {
        _UNSAFE.throwException(t);
    }

    /**
     * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
     * allow safepoint polling during a large copy.
     */
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;


    // APIs to read primitive data from a byte[] using Unsafe way

    /**
     * Converts a byte array to a short value considering it was written in big-endian format.
     *
     * @param bytes  byte array
     * @param offset offset into array
     * @return the short value
     */
    public static short toShort(byte[] bytes, int offset) {
        if (littleEndian) {
            return Short.reverseBytes(_UNSAFE.getShort(bytes, offset + BYTE_ARRAY_OFFSET));
        } else {
            return _UNSAFE.getShort(bytes, offset + BYTE_ARRAY_OFFSET);
        }
    }

    /**
     * Converts a byte array to an int value considering it was written in big-endian format.
     *
     * @param bytes  byte array
     * @param offset offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
        if (littleEndian) {
            return Integer.reverseBytes(_UNSAFE.getInt(bytes, offset + BYTE_ARRAY_OFFSET));
        } else {
            return _UNSAFE.getInt(bytes, offset + BYTE_ARRAY_OFFSET);
        }
    }

    /**
     * Converts a byte array to a long value considering it was written in big-endian format.
     *
     * @param bytes  byte array
     * @param offset offset into array
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset) {
        if (littleEndian) {
            return Long.reverseBytes(_UNSAFE.getLong(bytes, offset + BYTE_ARRAY_OFFSET));
        } else {
            return _UNSAFE.getLong(bytes, offset + BYTE_ARRAY_OFFSET);
        }
    }

    // APIs to write primitive data to a byte[] using Unsafe way

    /**
     * Put a short value out to the specified byte array position in big-endian format.
     *
     * @param bytes  the byte array
     * @param offset position in the array
     * @param val    short to write out
     * @return incremented offset
     */
    public static int putShort(byte[] bytes, int offset, short val) {
        if (littleEndian) {
            val = Short.reverseBytes(val);
        }
        _UNSAFE.putShort(bytes, offset + BYTE_ARRAY_OFFSET, val);
        return offset + BytesTools.SIZEOF_SHORT;
    }

    /**
     * Put an int value out to the specified byte array position in big-endian format.
     *
     * @param bytes  the byte array
     * @param offset position in the array
     * @param val    int to write out
     * @return incremented offset
     */
    public static int putInt(byte[] bytes, int offset, int val) {
        if (littleEndian) {
            val = Integer.reverseBytes(val);
        }
        _UNSAFE.putInt(bytes, offset + BYTE_ARRAY_OFFSET, val);
        return offset + BytesTools.SIZEOF_INT;
    }

    /**
     * Put a long value out to the specified byte array position in big-endian format.
     *
     * @param bytes  the byte array
     * @param offset position in the array
     * @param val    long to write out
     * @return incremented offset
     */
    public static int putLong(byte[] bytes, int offset, long val) {
        if (littleEndian) {
            val = Long.reverseBytes(val);
        }
        _UNSAFE.putLong(bytes, offset + BYTE_ARRAY_OFFSET, val);
        return offset + BytesTools.SIZEOF_LONG;
    }

    // APIs to read primitive data from a ByteBuffer using Unsafe way

    /**
     * Reads a short value at the given buffer's offset considering it was written in big-endian
     * format.
     *
     * @param buf
     * @param offset
     * @return short value at offset
     */
    public static short toShort(ByteBuffer buf, int offset) {
        if (littleEndian) {
            return Short.reverseBytes(getAsShort(buf, offset));
        }
        return getAsShort(buf, offset);
    }

    /**
     * Reads a short value at the given Object's offset considering it was written in big-endian
     * format.
     *
     * @param ref
     * @param offset
     * @return short value at offset
     */
    public static short toShort(Object ref, long offset) {
        if (littleEndian) {
            return Short.reverseBytes(_UNSAFE.getShort(ref, offset));
        }
        return _UNSAFE.getShort(ref, offset);
    }

    /**
     * Reads bytes at the given offset as a short value.
     *
     * @param buf
     * @param offset
     * @return short value at offset
     */
    static short getAsShort(ByteBuffer buf, int offset) {
        if (buf.isDirect()) {
            return _UNSAFE.getShort(((DirectBuffer) buf).address() + offset);
        }
        return _UNSAFE.getShort(buf.array(), BYTE_ARRAY_OFFSET + buf.arrayOffset() + offset);
    }

    /**
     * Reads an int value at the given buffer's offset considering it was written in big-endian
     * format.
     *
     * @param buf
     * @param offset
     * @return int value at offset
     */
    public static int toInt(ByteBuffer buf, int offset) {
        if (littleEndian) {
            return Integer.reverseBytes(getAsInt(buf, offset));
        }
        return getAsInt(buf, offset);
    }

    /**
     * Reads a int value at the given Object's offset considering it was written in big-endian
     * format.
     *
     * @param ref
     * @param offset
     * @return int value at offset
     */
    public static int toInt(Object ref, long offset) {
        if (littleEndian) {
            return Integer.reverseBytes(_UNSAFE.getInt(ref, offset));
        }
        return _UNSAFE.getInt(ref, offset);
    }

    /**
     * Reads bytes at the given offset as an int value.
     *
     * @param buf
     * @param offset
     * @return int value at offset
     */
    static int getAsInt(ByteBuffer buf, int offset) {
        if (buf.isDirect()) {
            return _UNSAFE.getInt(((DirectBuffer) buf).address() + offset);
        }
        return _UNSAFE.getInt(buf.array(), BYTE_ARRAY_OFFSET + buf.arrayOffset() + offset);
    }

    /**
     * Reads a long value at the given buffer's offset considering it was written in big-endian
     * format.
     *
     * @param buf
     * @param offset
     * @return long value at offset
     */
    public static long toLong(ByteBuffer buf, int offset) {
        if (littleEndian) {
            return Long.reverseBytes(getAsLong(buf, offset));
        }
        return getAsLong(buf, offset);
    }

    /**
     * Reads a long value at the given Object's offset considering it was written in big-endian
     * format.
     *
     * @param ref
     * @param offset
     * @return long value at offset
     */
    public static long toLong(Object ref, long offset) {
        if (littleEndian) {
            return Long.reverseBytes(_UNSAFE.getLong(ref, offset));
        }
        return _UNSAFE.getLong(ref, offset);
    }

    /**
     * Reads bytes at the given offset as a long value.
     *
     * @param buf
     * @param offset
     * @return long value at offset
     */
    static long getAsLong(ByteBuffer buf, int offset) {
        if (buf.isDirect()) {
            return _UNSAFE.getLong(((DirectBuffer) buf).address() + offset);
        }
        return _UNSAFE.getLong(buf.array(), BYTE_ARRAY_OFFSET + buf.arrayOffset() + offset);
    }

    /**
     * Put an int value out to the specified ByteBuffer offset in big-endian format.
     *
     * @param buf    the ByteBuffer to write to
     * @param offset offset in the ByteBuffer
     * @param val    int to write out
     * @return incremented offset
     */
    public static int putInt(ByteBuffer buf, int offset, int val) {
        if (littleEndian) {
            val = Integer.reverseBytes(val);
        }
        if (buf.isDirect()) {
            _UNSAFE.putInt(((DirectBuffer) buf).address() + offset, val);
        } else {
            _UNSAFE.putInt(buf.array(), offset + buf.arrayOffset() + BYTE_ARRAY_OFFSET, val);
        }
        return offset + BytesTools.SIZEOF_INT;
    }

    // APIs to copy data. This will be direct memory location copy and will be much faster

    /**
     * Copies the bytes from given array's offset to length part into the given buffer.
     *
     * @param src
     * @param srcOffset
     * @param dest
     * @param destOffset
     * @param length
     */
    public static void copy(byte[] src, int srcOffset, ByteBuffer dest, int destOffset, int length) {
        long destAddress = destOffset;
        Object destBase = null;
        if (dest.isDirect()) {
            destAddress = destAddress + ((DirectBuffer) dest).address();
        } else {
            destAddress = destAddress + BYTE_ARRAY_OFFSET + dest.arrayOffset();
            destBase = dest.array();
        }
        long srcAddress = srcOffset + BYTE_ARRAY_OFFSET;
        unsafeCopy(src, srcAddress, destBase, destAddress, length);
    }

    private static void unsafeCopy(Object src, long srcAddr, Object dst, long destAddr, long len) {
        while (len > 0) {
            long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
            _UNSAFE.copyMemory(src, srcAddr, dst, destAddr, len);
            len -= size;
            srcAddr += size;
            destAddr += size;
        }
    }

    /**
     * Copies specified number of bytes from given offset of {@code src} ByteBuffer to the
     * {@code dest} array.
     *
     * @param src
     * @param srcOffset
     * @param dest
     * @param destOffset
     * @param length
     */
    public static void copy(ByteBuffer src, int srcOffset, byte[] dest, int destOffset,
                            int length) {
        long srcAddress = srcOffset;
        Object srcBase = null;
        if (src.isDirect()) {
            srcAddress = srcAddress + ((DirectBuffer) src).address();
        } else {
            srcAddress = srcAddress + BYTE_ARRAY_OFFSET + src.arrayOffset();
            srcBase = src.array();
        }
        long destAddress = destOffset + BYTE_ARRAY_OFFSET;
        unsafeCopy(srcBase, srcAddress, dest, destAddress, length);
    }

    /**
     * Copies specified number of bytes from given offset of {@code src} buffer into the {@code dest}
     * buffer.
     *
     * @param src
     * @param srcOffset
     * @param dest
     * @param destOffset
     * @param length
     */
    public static void copy(ByteBuffer src, int srcOffset, ByteBuffer dest, int destOffset,
                            int length) {
        long srcAddress, destAddress;
        Object srcBase = null, destBase = null;
        if (src.isDirect()) {
            srcAddress = srcOffset + ((DirectBuffer) src).address();
        } else {
            srcAddress = srcOffset + src.arrayOffset() + BYTE_ARRAY_OFFSET;
            srcBase = src.array();
        }
        if (dest.isDirect()) {
            destAddress = destOffset + ((DirectBuffer) dest).address();
        } else {
            destAddress = destOffset + BYTE_ARRAY_OFFSET + dest.arrayOffset();
            destBase = dest.array();
        }
        unsafeCopy(srcBase, srcAddress, destBase, destAddress, length);
    }

    // APIs to add primitives to BBs

    /**
     * Put a short value out to the specified BB position in big-endian format.
     *
     * @param buf    the byte buffer
     * @param offset position in the buffer
     * @param val    short to write out
     * @return incremented offset
     */
    public static int putShort(ByteBuffer buf, int offset, short val) {
        if (littleEndian) {
            val = Short.reverseBytes(val);
        }
        if (buf.isDirect()) {
            _UNSAFE.putShort(((DirectBuffer) buf).address() + offset, val);
        } else {
            _UNSAFE.putShort(buf.array(), BYTE_ARRAY_OFFSET + buf.arrayOffset() + offset, val);
        }
        return offset + BytesTools.SIZEOF_SHORT;
    }

    /**
     * Put a long value out to the specified BB position in big-endian format.
     *
     * @param buf    the byte buffer
     * @param offset position in the buffer
     * @param val    long to write out
     * @return incremented offset
     */
    public static int putLong(ByteBuffer buf, int offset, long val) {
        if (littleEndian) {
            val = Long.reverseBytes(val);
        }
        if (buf.isDirect()) {
            _UNSAFE.putLong(((DirectBuffer) buf).address() + offset, val);
        } else {
            _UNSAFE.putLong(buf.array(), BYTE_ARRAY_OFFSET + buf.arrayOffset() + offset, val);
        }
        return offset + BytesTools.SIZEOF_LONG;
    }

    /**
     * Put a byte value out to the specified BB position in big-endian format.
     *
     * @param buf    the byte buffer
     * @param offset position in the buffer
     * @param b      byte to write out
     * @return incremented offset
     */
    public static int putByte(ByteBuffer buf, int offset, byte b) {
        if (buf.isDirect()) {
            _UNSAFE.putByte(((DirectBuffer) buf).address() + offset, b);
        } else {
            _UNSAFE.putByte(buf.array(),
                    BYTE_ARRAY_OFFSET + buf.arrayOffset() + offset, b);
        }
        return offset + 1;
    }

    /**
     * Returns the byte at the given offset
     *
     * @param buf    the buffer to read
     * @param offset the offset at which the byte has to be read
     * @return the byte at the given offset
     */
    public static byte toByte(ByteBuffer buf, int offset) {
        if (buf.isDirect()) {
            return _UNSAFE.getByte(((DirectBuffer) buf).address() + offset);
        } else {
            return _UNSAFE.getByte(buf.array(), BYTE_ARRAY_OFFSET + buf.arrayOffset() + offset);
        }
    }

    /**
     * Returns the byte at the given offset of the object
     *
     * @param ref
     * @param offset
     * @return the byte at the given offset
     */
    public static byte toByte(Object ref, long offset) {
        return _UNSAFE.getByte(ref, offset);
    }

    static {
        Unsafe unsafe;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
        } catch (Throwable cause) {
            unsafe = null;
        }
        _UNSAFE = unsafe;

        if (_UNSAFE != null) {
            BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
            SHORT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(short[].class);
            INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
            LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
            FLOAT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(float[].class);
            DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
        } else {
            BYTE_ARRAY_OFFSET = 0;
            SHORT_ARRAY_OFFSET = 0;
            INT_ARRAY_OFFSET = 0;
            LONG_ARRAY_OFFSET = 0;
            FLOAT_ARRAY_OFFSET = 0;
            DOUBLE_ARRAY_OFFSET = 0;
        }
    }

    public static long objectFieldOffset(Field field) {
        return _UNSAFE.objectFieldOffset(field);
    }

    public static void putOrderedLong(Object object, long valueOffset, long initialValue) {
        _UNSAFE.putOrderedLong(object, valueOffset, initialValue);
    }

    public static void putLongVolatile(Object object, long valueOffset, long value) {
        _UNSAFE.putLongVolatile(object, valueOffset, value);
    }

    public static boolean compareAndSwapLong(Object object, long valueOffset, long expectedValue, long newValue) {
        return _UNSAFE.compareAndSwapLong(object, valueOffset, expectedValue, newValue);
    }

    public static int arrayBaseOffset(Class aClass) {
        return _UNSAFE.arrayBaseOffset(aClass);
    }

    public static int arrayIndexScale(Class aClass) {
        return _UNSAFE.arrayIndexScale(aClass);
    }

    public static void putOrderedInt(Object availableBuffer, long bufferAddress, int flag) {
        _UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    public static int getIntVolatile(Object availableBuffer, long bufferAddress) {
        return _UNSAFE.getIntVolatile(availableBuffer, bufferAddress);
    }

    public static Object getObject(Object entries, long l) {
        return _UNSAFE.getObject(entries, l);
    }

    public static char getChar(Object baseObj, long l) {
        return _UNSAFE.getChar(baseObj, l);
    }

    public static void putChar(Object baseObj, long l, char value) {
        _UNSAFE.putChar(baseObj, l, value);
    }
}
