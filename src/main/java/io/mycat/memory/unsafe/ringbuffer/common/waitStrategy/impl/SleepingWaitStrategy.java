package io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.impl;

import io.mycat.memory.unsafe.ringbuffer.common.barrier.SequenceBarrier;
import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.WaitStrategy;
import io.mycat.memory.unsafe.ringbuffer.exception.AlertException;

import java.util.concurrent.locks.LockSupport;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/8/1
 */
public class SleepingWaitStrategy implements WaitStrategy {
    //重试200次
    private static final int DEFAULT_RETRIES = 200;
    private final int retries;

    public SleepingWaitStrategy() {
        this(DEFAULT_RETRIES);
    }

    public SleepingWaitStrategy(int retries) {
        this.retries = retries;
    }

    @Override
    public long waitFor(
            final long sequence, Sequence cursor, final Sequence dependentSequence, final SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        int counter = retries;
        //直接检查dependentSequence.get() < sequence
        while ((availableSequence = dependentSequence.get()) < sequence) {
            counter = applyWaitMethod(barrier, counter);
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
    }

    private int applyWaitMethod(final SequenceBarrier barrier, int counter)
            throws AlertException {
        //检查是否需要终止
        barrier.checkAlert();
        //如果在200~100,重试
        if (counter > 100) {
            --counter;
        }
        //如果在100~0,调用Thread.yield()让出CPU
        else if (counter > 0) {
            --counter;
            Thread.yield();
        }
        //<0的话，利用LockSupport.parkNanos(1L)来sleep最小时间
        else {
            LockSupport.parkNanos(1L);
        }
        return counter;
    }
}
