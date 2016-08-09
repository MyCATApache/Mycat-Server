package io.mycat.memory.unsafe.ringbuffer.producer;

import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.WaitStrategy;
import io.mycat.memory.unsafe.ringbuffer.exception.InsufficientCapacityException;
import io.mycat.memory.unsafe.ringbuffer.utils.Util;

import java.util.concurrent.locks.LockSupport;

/**
 * 单一生产者相关类，非线程安全
 *
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/24
 */

abstract class SingleProducerSequencerPad extends AbstractSequencer
{
    protected long p1, p2, p3, p4, p5, p6, p7;

    public SingleProducerSequencerPad(int bufferSize, WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad
{
    public SingleProducerSequencerFields(int bufferSize, WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    protected long nextValue = Sequence.INITIAL_VALUE;
    protected long cachedValue = Sequence.INITIAL_VALUE;
}

public class SingleProducerSequencer extends SingleProducerSequencerFields{

    public SingleProducerSequencer(int bufferSize, final WaitStrategy waitStrategy) {
        super(bufferSize, waitStrategy);
    }

    @Override
    public void claim(long sequence) {
        nextValue = sequence;
    }

    @Override
    public boolean isAvailable(long sequence) {
        return sequence <= cursor.get();
    }

    @Override
    public long getHighestPublishedSequence(long nextSequence, long availableSequence) {
        return availableSequence;
    }

    @Override
    public boolean hasAvailableCapacity(int requiredCapacity) {
        //下一个生产Sequence位置
        long nextValue = this.nextValue;
        //下一位置加上所需容量减去整个bufferSize，如果为正数，那证明至少转了一圈，则需要检查gatingSequences（由消费者更新里面的Sequence值）以保证不覆盖还未被消费的
        long wrapPoint = (nextValue + requiredCapacity) - bufferSize;
        //Disruptor经常用缓存，这里缓存之间所有gatingSequences最小的那个，这样不用每次都遍历一遍gatingSequences，影响效率
        long cachedGatingSequence = this.cachedValue;
        //只要wrapPoint大于缓存的所有gatingSequences最小的那个，就重新检查更新缓存
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            long minSequence = Util.getMinimumSequence(gatingSequences, nextValue);
            this.cachedValue = minSequence;
            //空间不足返回false
            if (wrapPoint > minSequence)
            {
                return false;
            }
        }
        //若wrapPoint小于缓存的所有gatingSequences最小的那个，证明可以放心生产
        return true;
    }

    @Override
    public long remainingCapacity() {
        //使用的 = 生产的 - 已经消费的
        //剩余容量 = 容量 - 使用的
        long nextValue = this.nextValue;
        long consumed = Util.getMinimumSequence(gatingSequences, nextValue);
        long produced = nextValue;
        return getBufferSize() - (produced - consumed);
    }

    @Override
    public long next() {
        return next(1);
    }

    @Override
    public long next(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be > 0");
        }

        long nextValue = this.nextValue;
        //next方法和之前的hasAvailableCapacity同理，只不过这里是相当于阻塞的
        long nextSequence = nextValue + n;
        long wrapPoint = nextSequence - bufferSize;
        long cachedGatingSequence = this.cachedValue;

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue) {
            long minSequence;
            //只要wrapPoint大于最小的gatingSequences，那么不断唤醒消费者去消费，并利用LockSupport让出CPU，直到wrapPoint不大于最小的gatingSequences
            while (wrapPoint > (minSequence = Util.getMinimumSequence(gatingSequences, nextValue))) {
                waitStrategy.signalAllWhenBlocking();
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin?
            }
            //同理，缓存最小的gatingSequences
            this.cachedValue = minSequence;
        }

        this.nextValue = nextSequence;

        return nextSequence;
    }

    @Override
    public long tryNext() throws InsufficientCapacityException {
        return tryNext(1);
    }

    @Override
    public long tryNext(int n) throws InsufficientCapacityException {
        if (n < 1) {
            throw new IllegalArgumentException("n must be > 0");
        }

        if (!hasAvailableCapacity(n)) {
            throw InsufficientCapacityException.INSTANCE;
        }

        long nextSequence = this.nextValue += n;

        return nextSequence;
    }

    @Override
    public void publish(long sequence) {
        //cursor代表可以消费的sequence
        cursor.set(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void publish(long lo, long hi) {
        publish(hi);
    }
}
