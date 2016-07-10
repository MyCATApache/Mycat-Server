package io.mycat.memory.helper;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * Simple map class which stores two longs, x and y.
 *
 * <pre>
 * 0x00000000: 01 00 00 00 00 00 00 00  AE C4 62 DF 00 00 00 00
 * 0x00000010: 01 00 00 00 00 00 00 00  02 00 00 00 00 00 00 00
 * </pre>
 */
public class LongPoint implements Comparable<LongPoint> {
  public long x;
  public long y;

  public LongPoint(long x, long y) {
    this.x = x;
    this.y = y;
  }

  public String toString() {
    return "Point(" + x + "," + y + ")";
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    LongPoint longPoint = (LongPoint) obj;
    return Objects.equal(x, longPoint.x) && Objects.equal(y, longPoint.y);
  }

  @Override public int hashCode() {
    return Objects.hashCode(x, y);
  }

  public int compareTo(LongPoint obj) {
    return ComparisonChain.start().compare(x, obj.x).compare(y, obj.y).result();
  }
}
