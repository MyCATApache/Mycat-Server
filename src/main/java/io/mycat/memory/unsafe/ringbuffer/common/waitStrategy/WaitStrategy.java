package io.mycat.memory.unsafe.ringbuffer.common.waitStrategy;

import io.mycat.memory.unsafe.ringbuffer.common.barrier.SequenceBarrier;
import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.exception.AlertException;
import io.mycat.memory.unsafe.ringbuffer.exception.TimeoutException;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/24
 */
public interface WaitStrategy {
    /**
     * @param sequence 需要等待available的sequence
     * @param cursor 对应RingBuffer的Cursor
     * @param dependentSequence 需要等待（依赖）的Sequence
     * @param barrier 多消费者注册的SequenceBarrier
     * @return 已经available的sequence
     * @throws AlertException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException;

    /**
     * 唤醒所有等待的消费者
     */
    void signalAllWhenBlocking();
}
