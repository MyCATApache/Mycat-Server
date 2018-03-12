package io.mycat.memory.unsafe.ringbuffer.common.barrier;

import io.mycat.memory.unsafe.ringbuffer.exception.AlertException;
import io.mycat.memory.unsafe.ringbuffer.exception.TimeoutException;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/24
 */
public interface SequenceBarrier {
    /**
     * 等待给定的sequence值可以被消费
     *
     * @param sequence 等待的sequence值
     * @return 可以消费的最大sequence值
     * @throws AlertException 当Disruptor的状态改变时会抛出
     * @throws InterruptedException 唤醒线程
     * @throws TimeoutException 超过最大等待时间
     */
    long waitFor(long sequence) throws AlertException, InterruptedException, TimeoutException;

    /**
     * 获取当前可以消费的cursor值
     *
     * @return 当前可以消费的cursor值（已经被publish的）
     */
    long getCursor();

    /**
     * alert状态
     *
     * @return true 如果被alerted
     */
    boolean isAlerted();

    /**
     * 进入alert状态
     */
    void alert();

    /**
     * 清除当前alert状态
     */
    void clearAlert();

    /**
     * 检查是否被alerted，如果是，则抛出{@link AlertException}
     *
     * @throws AlertException if alert has been raised.
     */
    void checkAlert() throws AlertException;
}
