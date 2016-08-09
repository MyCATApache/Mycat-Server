package io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.impl;

import io.mycat.memory.unsafe.ringbuffer.common.barrier.SequenceBarrier;
import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.WaitStrategy;
import io.mycat.memory.unsafe.ringbuffer.exception.AlertException;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/8/1
 */
public class BusySpinWaitStrategy implements WaitStrategy {
    @Override
    public long waitFor(
            final long sequence, Sequence cursor, final Sequence dependentSequence, final SequenceBarrier barrier)
            throws AlertException, InterruptedException {

        long availableSequence;
        //一直while自旋检查
        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
        }
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
    }
}
