package org.opencloudb.stat;

import java.util.concurrent.atomic.AtomicLongArray;

public class Histogram {

    private final long[] ranges;
    private final AtomicLongArray rangeCounters;

    public Histogram(long... ranges){
        this.ranges = ranges;
        this.rangeCounters = new AtomicLongArray(ranges.length);
    }

    public void reset() {
        for (int i = 0; i < rangeCounters.length(); i++) {
            rangeCounters.set(i, 0);
        }
    }

    public void record(long range) {
        int index = rangeCounters.length();
        for (int i = 0; i < ranges.length; i++) {
            if (range == ranges[i]) {
                index = i;
                break;
            }
        }

        rangeCounters.incrementAndGet(index);
    }

    public long get(int index) {
        return rangeCounters.get(index);
    }

    public long[] toArray() {
        long[] array = new long[rangeCounters.length()];
        for (int i = 0; i < rangeCounters.length(); i++) {
            array[i] = rangeCounters.get(i);
        }
        return array;
    }

    public long[] toArrayAndReset() {
        long[] array = new long[rangeCounters.length()];
        for (int i = 0; i < rangeCounters.length(); i++) {
            array[i] = rangeCounters.getAndSet(i, 0);
        }

        return array;
    }

    public long[] getRanges() {
        return ranges;
    }

    public long getValue(int index) {
        return rangeCounters.get(index);
    }

    public long getSum() {
        long sum = 0;
        for (int i = 0; i < rangeCounters.length(); ++i) {
            sum += rangeCounters.get(i);
        }
        return sum;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append('[');
        for (int i = 0; i < rangeCounters.length(); i++) {
            if (i != 0) {
                buf.append(", ");
            }
            buf.append(rangeCounters.get(i));
        }
        buf.append(']');
        return buf.toString();
    }
}
