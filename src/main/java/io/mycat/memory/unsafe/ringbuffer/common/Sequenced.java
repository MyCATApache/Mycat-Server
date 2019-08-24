package io.mycat.memory.unsafe.ringbuffer.common;

import io.mycat.memory.unsafe.ringbuffer.exception.InsufficientCapacityException;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/23
 */
public interface Sequenced {
    /**
     * @return ringBuffer的大小
     */
    int getBufferSize();

    /**
     * @param requiredCapacity 需要的大小
     * @return true ringBuffer的剩余空间足够 | false ringBuffer的剩余空间不足
     */
    boolean hasAvailableCapacity(final int requiredCapacity);

    /**
     * @return ringBuffer的剩余空间
     */
    long remainingCapacity();

    /**
     * 申请下一个sequence(value)作为生产event的位置
     * @return sequence的value
     */
    long next();

    /**
     * 申请下n个sequence(value)作为生产多个event的位置
     * @param n
     * @return 最高的sequence的value
     */
    long next(int n);
    /**
     * 尝试申请下一个sequence(value)作为生产event的位置
     * @return sequence的value
     * @throws InsufficientCapacityException
     */
    long tryNext() throws InsufficientCapacityException;

    /**
     * 尝试申请下n个sequence(value)作为生产多个event的位置
     * @param n
     * @return 最高的sequence的value
     * @throws InsufficientCapacityException
     */
    long tryNext(int n) throws InsufficientCapacityException;

    /**
     * 发布一个Sequence，一般在这个Sequence对应位置的Event被填充后
     * @param sequence
     */
    void publish(long sequence);

    /**
     * 发布多个Sequence，一般在这些Sequence对应位置的Event被填充后
     * @param lo 第一个sequence的value
     * @param hi 最后一个sequence的value
     */
    void publish(long lo, long hi);
}
