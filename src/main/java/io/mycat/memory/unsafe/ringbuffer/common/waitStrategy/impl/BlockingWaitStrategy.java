package io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.impl;

import io.mycat.memory.unsafe.ringbuffer.common.barrier.SequenceBarrier;
import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.WaitStrategy;
import io.mycat.memory.unsafe.ringbuffer.exception.AlertException;
import io.mycat.memory.unsafe.ringbuffer.exception.TimeoutException;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/8/1
 */
public class BlockingWaitStrategy implements WaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();

    @Override
    public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        if (cursorSequence.get() < sequence) {
            lock.lock();
            try {
                while (cursorSequence.get() < sequence) {
                    //检查是否Alert，如果Alert，则抛出AlertException
                    //Alert在这里代表对应的消费者被halt停止了
                    barrier.checkAlert();
                    //在processorNotifyCondition上等待唤醒
                    processorNotifyCondition.await();
                }
            } finally {
                lock.unlock();
            }
        }

        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        lock.lock();
        try {
            //生产者生产消息后，会唤醒所有等待的消费者线程
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
