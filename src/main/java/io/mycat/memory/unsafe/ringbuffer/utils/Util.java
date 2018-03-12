package io.mycat.memory.unsafe.ringbuffer.utils;

import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;

/**
 * Set of common functions used by the Disruptor
 */
public class Util {
    /**
     * 计算下一个不小于x的2的n次方
     * 原理：int最长为32位，计算x-1的前面有多少个0，之后用32减去这个值得到n，那么2的n次方就是下一个不小于x的2的n次方
     *
     * @param x Value to round up
     * @return The next power of 2 from x inclusive
     */
    public static int ceilingNextPowerOfTwo(final int x) {
        return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
    }

    /**
     * 获取Sequence数组中value最小的值
     *
     * @param sequences to compare.
     * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
     */
    public static long getMinimumSequence(final Sequence[] sequences)
    {
        return getMinimumSequence(sequences, Long.MAX_VALUE);
    }

    /**
     * 获取Sequence数组中value最小的值
     *
     * @param sequences to compare.
     * @param minimum   如果数组为空，将返回这个值
     * @return the smaller of minimum sequence value found in {@code sequences} and {@code minimum};
     * {@code minimum} if {@code sequences} is empty
     */
    public static long getMinimumSequence(final Sequence[] sequences, long minimum)
    {
        for (int i = 0, n = sequences.length; i < n; i++)
        {
            long value = sequences[i].get();
            minimum = Math.min(minimum, value);
        }

        return minimum;
    }

    public static int log2(int i)
    {
        int r = 0;
        while ((i >>= 1) != 0)
        {
            ++r;
        }
        return r;
    }

}
