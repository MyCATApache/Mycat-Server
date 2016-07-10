package io.mycat.memory.helper;


import sun.misc.Unsafe;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;

/**
 * Methods adapted from:
 * <ul>
 * <li><a href="http://mishadoff.com/blog/java-magic-part-4-sun-dot-misc-dot-unsafe/">http://mishadoff.com/blog/java-magic-part-4-sun-dot-misc-dot-unsafe/</a></li>
 * <li><a href="http://mydailyjava.blogspot.com/2013/12/sunmiscunsafe.html">http://mydailyjava.blogspot.com/2013/12/sunmiscunsafe.html</a></li>
 * <li><a href="http://zeroturnaround.com/rebellabs/dangerous-code-how-to-be-unsafe-with-java-classes-objects-in-memory/">http://zeroturnaround.com/rebellabs/dangerous-code-how-to-be-unsafe-with-java-classes-objects-in-memory/</a></li>
 * </ul>
 *
 * <p>Other interesting reads:
 * <ul>
 * <li><a href="http://www.codeinstructions.com/2008/12/java-objects-memory-structure.html">http://www.codeinstructions.com/2008/12/java-objects-memory-structure.html</a></li>
 * </ul>
 */
public class UnsafeHelper {

  private static final char[] hexArray = "0123456789ABCDEF".toCharArray();
  private static final long COPY_STRIDE = 8;

  private static final Unsafe unsafe = createUnsafe();

  private static Unsafe createUnsafe() {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      return (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Can't use unsafe", e);
    }
  }

  public static Unsafe getUnsafe() {
    return unsafe;
  }

  /**
   * Returns the address the object is located at
   *
   * <p>WARNING: This does not return a pointer, so be warned pointer arithmetic will not work.
   *
   * @param obj The object
   * @return the address of the object
   */
  public static long toAddress(Object obj) {
    Object[] array = new Object[] {obj};
    long baseOffset = unsafe.arrayBaseOffset(Object[].class);
    return normalize(unsafe.getInt(array, baseOffset));
  }

  /**
   * Returns the object located at the address.
   *
   * @param address The address
   * @return the object at this address
   */
  public static Object fromAddress(long address) {
    Object[] array = new Object[] {null};
    long baseOffset = unsafe.arrayBaseOffset(Object[].class);
    unsafe.putLong(array, baseOffset, address);
    return array[0];
  }

  public static void copyMemory(long srcAddress, Object dest) {
    copyMemory(null, srcAddress, dest, 0, sizeOf(dest));
  }

  /**
   * Copies the memory from srcAddress into dest
   *
   * <p>This is our own implementation because Unsafe.copyMemory(Object src, .. Object dest, ...)
   * only works if <a href="https://goo.gl/pBVlJv">dest in an array</a>, so we wrote our only
   * implementations.
   */
  public static void copyMemory(final Object src, long srcOffset, final Object dest,
      final long destOffset, final long len) {

    // TODO make this work if destOffset is not STRIDE aligned
    Preconditions.checkNotNull(src);
    Preconditions.checkArgument(len % COPY_STRIDE != 0, "Length (%d) is not a multiple of stride", len);

    Preconditions.checkArgument(destOffset % COPY_STRIDE != 0,
        "Dest offset (%d) is not stride aligned", destOffset);

    long end = destOffset + len;
    for (long offset = destOffset; offset < end; ) {
      unsafe.putLong(dest, offset, unsafe.getLong(srcOffset));
      offset += COPY_STRIDE;
      srcOffset += COPY_STRIDE;
    }
  }

  /**
   * Copies from srcAddress to dest one field at a time.
   *
   * @param srcAddress
   * @param dest
   */
  public static void copyMemoryFieldByField(long srcAddress, Object dest) {

    Class clazz = dest.getClass();
    while (clazz != Object.class) {
      for (Field f : clazz.getDeclaredFields()) {
        if ((f.getModifiers() & Modifier.STATIC) == 0) {
          final Class type = f.getType();

          // TODO maybe support Wrapper classes
          Preconditions.checkArgument(type.isPrimitive(), "Only primitives are supported");

          final long offset = unsafe.objectFieldOffset(f);
          final long src = srcAddress + offset;

          if (type == int.class) {
            unsafe.putInt(dest, offset, unsafe.getInt(src));

          } else if (type == long.class) {
            unsafe.putLong(dest, offset, unsafe.getLong(src));

          } else {
            throw new IllegalArgumentException("Type not supported yet: " + type);
          }
        }
      }
      clazz = clazz.getSuperclass();
    }
  }

  public static long jvm7_32_sizeOf(Object object) {
    // This is getting the size out of the class header (at offset 12)
    return unsafe.getAddress(normalize(unsafe.getInt(object, 4L)) + 12L);
  }

  public static long headerSize(Object obj) {
    return headerSize(obj.getClass());
  }

  public static long firstFieldOffset(Object obj) {
    return firstFieldOffset(obj.getClass());
  }

  public static long sizeOf(Object obj) {
    Class clazz = obj.getClass();

    long len = sizeOf(clazz);

    if (clazz.isArray()) {
      // TODO Do extra work
      // TODO move into sizeof(Object)
      // (8) first longs and doubles; then
      // (4) ints and floats; then
      // (2) chars and shorts; then
      // (1) bytes and booleans, and last the
      // (4-8) references.
      Object[] array = (Object[]) obj;
      len += array.length * 8;
    }

    return len;
  }

  public static long sizeOfFields(Object obj) {
    return sizeOfFields(obj.getClass());
  }

  private static long roundUpTo8(final long number) {
    return ((number + 7) / 8) * 8;
  }

  /**
   * Returns the size of the header for an instance of this class (in bytes).
   *
   * <p>More information <a href="http://www.codeinstructions.com/2008/12/java-objects-memory-structure.html">http://www.codeinstructions.com/2008/12/java-objects-memory-structure.html</a>
   * and <a href="http://stackoverflow.com/a/17348396/88646">http://stackoverflow.com/a/17348396/88646</a>
   *
   * <p><pre>
   * ,------------------+------------------+------------------ +---------------.
   * |    mark word(8)  | klass pointer(4) |  array size (opt) |    padding    |
   * `------------------+------------------+-------------------+---------------'
   * </pre>
   *
   * @param clazz
   * @return
   */
  public static long headerSize(Class clazz) {
    Preconditions.checkNotNull(clazz);
    // TODO Should be calculated based on the platform
    // TODO maybe unsafe.addressSize() would help?
    long len = 12; // JVM_64 has a 12 byte header 8 + 4 (with compressed pointers on)
    if (clazz.isArray()) {
      len += 4;
    }
    return len;
  }

  /**
   * Returns the offset of the first field in the range [headerSize, sizeOf].
   *
   * @param clazz
   * @return
   */
  public static long firstFieldOffset(Class clazz) {
    long minSize = roundUpTo8(headerSize(clazz));

    // Find the min offset for all the classes, up the class hierarchy.
    while (clazz != Object.class) {
      for (Field f : clazz.getDeclaredFields()) {
        if ((f.getModifiers() & Modifier.STATIC) == 0) {
          long offset = unsafe.objectFieldOffset(f);
          if (offset < minSize) {
            minSize = offset;
          }
        }
      }
      clazz = clazz.getSuperclass();
    }

    return minSize;
  }

  /**
   * Returns the size of an instance of this class (in bytes).
   * Instances include a header + all fields + padded to 8 bytes.
   * If this is an array, it does not include the size of the elements.
   *
   * @param clazz
   * @return
   */
  public static long sizeOf(Class clazz) {
    long maxSize = headerSize(clazz);

    while (clazz != Object.class) {
      for (Field f : clazz.getDeclaredFields()) {
        if ((f.getModifiers() & Modifier.STATIC) == 0) {
          long offset = unsafe.objectFieldOffset(f);
          if (offset > maxSize) {
            // Assume 1 byte of the field width. This is ok as it gets padded out at the end
            maxSize = offset + 1;
          }
        }
      }
      clazz = clazz.getSuperclass();
    }

    // The whole class always pads to a 8 bytes boundary, so we round up to 8 bytes.
    return roundUpTo8(maxSize);
  }

  /**
   * Size of all the fields
   * @param clazz
   * @return
   */
  public static long sizeOfFields(Class clazz) {
    return sizeOf(clazz) - firstFieldOffset(clazz);
  }

  private static long normalize(int value) {
    if (value >= 0) {
      return value;
    }
    return (~0L >>> 32) & value;
  }

  /**
   * Returns the object as a byte array, including header, padding and all fields.
   *
   * @param obj
   * @return
   */
  public static byte[] toByteArray(Object obj) {
    int len = (int) sizeOf(obj);
    byte[] bytes = new byte[len];
    unsafe.copyMemory(obj, 0, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, bytes.length);
    return bytes;
  }

  /**
   * Prints out the object (including header, padding, and all fields) as hex.
   *
   * <p>Some examples:
   *
   * <p><pre>
   * /**
   *  * Longs are always 8 byte aligned, so 4 bytes of padding
   *  * 0x00000000: 01 00 00 00 00 00 00 00  9B 81 61 DF 00 00 00 00
   *  * 0x00000010: EF CD AB 89 67 45 23 01
   *  *&#47;
   * static class Class8 {
   *   long l = 0x0123456789ABCDEFL;
   * }
   *
   * /**
   *  * 0x00000000: 01 00 00 00 00 00 00 00  8A BF 62 DF 67 45 23 01
   *  *&#47;
   * static class Class4 {
   *   int i = 0x01234567;
   * }
   *
   * /**
   *  * 0x00000000: 01 00 00 00 00 00 00 00  28 C0 62 DF 34 12 00 00
   *  *&#47;
   * static class Class2 {
   *   short s = 0x01234;
   * }
   *
   * /**
   *  * 0x00000000: 01 00 00 00 00 00 00 00  E3 C0 62 DF 12 00 00 00
   *  *&#47;
   * static class Class1 {
   *   byte b = 0x12;
   * }
   *
   * /**
   *  * 0x00000000: 01 00 00 00 00 00 00 00  96 C1 62 DF 12 00 00 00
   *  * 0x00000010: EF CD AB 89 67 45 23 01
   *  *&#47;
   * static class ClassMixed18 {
   *   byte b = 0x12;
   *   long l = 0x0123456789ABCDEFL;
   * }
   *
   * /**
   *  * 0x00000000: 01 00 00 00 00 00 00 00  4C C2 62 DF 12 00 00 00
   *  * 0x00000010: EF CD AB 89 67 45 23 01
   *  *&#47;
   * static class ClassMixed81 {
   *   long l = 0x0123456789ABCDEFL;
   *   byte b = 0x12;
   * }
   *
   * public static void printMemoryLayout() {
   *   UnsafeHelper.hexDump(System.out, new Class8());
   *   UnsafeHelper.hexDump(System.out, new Class4());
   *   UnsafeHelper.hexDump(System.out, new Class2());
   *   UnsafeHelper.hexDump(System.out, new Class1());
   *   UnsafeHelper.hexDump(System.out, new ClassMixed18());
   *   UnsafeHelper.hexDump(System.out, new ClassMixed81());
   * }
   * </pre>
   *
   * @param out PrintStream to print the hex to
   * @param obj The object to print
   */
  public static void hexDump(PrintStream out, Object obj) {
    // TODO Change this to use hexDumpAddress instead of toByteArray
    byte[] bytes = toByteArray(obj);
    hexDumpBytes(out, 0, bytes);
  }

  public static String hexDump(Object obj) throws UnsupportedEncodingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    hexDump(new PrintStream(baos), obj);

    return baos.toString(StandardCharsets.UTF_8.name());
  }

  public static void hexDumpBytes(PrintStream out, long offset, byte[] bytes) {
    final int lineWidth = 16;
    char[] line = new char[lineWidth * 3];

    for (int i = 0; i < bytes.length; i += lineWidth) {
      int len = Math.min(bytes.length - i, 16);

      for (int j = 0; j < len; j++) {
        int value = bytes[i + j] & 0xFF;
        line[j * 3 + 0] = hexArray[value >>> 4];
        line[j * 3 + 1] = hexArray[value & 0x0F];
        line[j * 3 + 2] = ' ';
      }

      int len1 = Math.min(len, 8) * 3;
      int len2 = Math.min(len - 8, 8) * 3;
      out.printf("0x%08X: %s %s%n", offset + (long) i, new String(line, 0, len1),
          new String(line, 8 * 3, len2));
    }
  }

  public static void hexDumpAddress(PrintStream out, long address, long length) {
    byte[] bytes = new byte[16];
    while (length > 0) {
      long chunk = Math.min(bytes.length, length);
      unsafe.copyMemory(null, address, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, chunk);
      hexDumpBytes(out, address, bytes);
      length -= chunk;
      address += chunk;
    }
  }
}
