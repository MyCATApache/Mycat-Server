

package io.mycat.memory.unsafe.utils.sort;

import com.google.common.primitives.UnsignedLongs;
import io.mycat.memory.unsafe.types.ByteArray;


public class PrefixComparators {
  private PrefixComparators() {}

  public static final PrefixComparator STRING = new UnsignedPrefixComparator();
  public static final PrefixComparator STRING_DESC = new UnsignedPrefixComparatorDesc();
  public static final PrefixComparator BINARY = new UnsignedPrefixComparator();
  public static final PrefixComparator BINARY_DESC = new UnsignedPrefixComparatorDesc();
  public static final PrefixComparator LONG = new SignedPrefixComparator();
  public static final PrefixComparator LONG_DESC = new SignedPrefixComparatorDesc();
  public static final PrefixComparator DOUBLE = new UnsignedPrefixComparator();
  public static final PrefixComparator DOUBLE_DESC = new UnsignedPrefixComparatorDesc();

  public static final PrefixComparator RadixSortDemo = new RadixSortDemo();



  public static final class BinaryPrefixComparator {
    public static long computePrefix(byte[] bytes) {
      return ByteArray.getPrefix(bytes);
    }
  }

  public static final class DoublePrefixComparator {
    /**
     * Converts the double into a value that compares correctly as an unsigned long. For more
     * details see http://stereopsis.com/radix.html.
     */
    public static long computePrefix(double value) {
      // Java's doubleToLongBits already canonicalizes all NaN values to the smallest possible
      // positive NaN, so there's nothing special we need to do for NaNs.
      long bits = Double.doubleToLongBits(value);
      // Negative floats compare backwards due to their sign-magnitude representation, so flip
      // all the bits in this case.
      long mask = -(bits >>> 63) | 0x8000000000000000L;
      return bits ^ mask;
    }
  }

  /**
   * Provides radix sort parameters. Comparators implementing this also are indicating that the
   * ordering they define is compatible with radix sort.
   */
  public abstract static class RadixSortSupport extends PrefixComparator {
    /** @return Whether the sort should be descending in binary sort order. */
    public abstract boolean sortDescending();

    /** @return Whether the sort should take into account the sign bit. */
    public abstract boolean sortSigned();
  }

  public static final  class RadixSortDemo extends PrefixComparators.RadixSortSupport{

    @Override
    public boolean sortDescending() {
      return false;
    }

    @Override
    public boolean sortSigned() {
      return false;
    }

    @Override
    public int compare(long prefix1, long prefix2) {
      return PrefixComparators.BINARY.compare(prefix1 & 0xffffff0000L, prefix1 & 0xffffff0000L);
    }
  }
  //
  // Standard prefix comparator implementations
  //

  public static final class UnsignedPrefixComparator extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return false; }
    @Override
    public int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class UnsignedPrefixComparatorDesc extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return false; }
    @Override
    public int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  public static final class SignedPrefixComparator extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return true; }
    @Override
    public int compare(long a, long b) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
  }

  public static final class SignedPrefixComparatorDesc extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return true; }
    @Override
    public int compare(long b, long a) {
      return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
  }
}
