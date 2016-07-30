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
    long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException;

    void signalAllWhenBlocking();
}
