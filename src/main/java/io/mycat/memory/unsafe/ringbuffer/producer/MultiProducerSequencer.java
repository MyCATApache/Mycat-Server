package io.mycat.memory.unsafe.ringbuffer.producer;

import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.WaitStrategy;
import io.mycat.memory.unsafe.ringbuffer.exception.InsufficientCapacityException;
import io.mycat.memory.unsafe.ringbuffer.utils.Util;

import java.util.concurrent.locks.LockSupport;

/**
 * 多生产者类，线程安全，与单一生产者不同的是，这里的cursor不再是可以消费的标记，而是多线程生产者抢占的标记
 * 可以消费的sequence由availableBuffer来判断标识
 *
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/24
 */
public class MultiProducerSequencer extends AbstractSequencer{
    private static final long BASE = Platform.arrayBaseOffset(int[].class);
    private static final long SCALE = Platform.arrayIndexScale(int[].class);

    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    private final int[] availableBuffer;
    //利用对2^n取模 = 对2^n -1 取与运算原理，indexMask=bufferSize - 1
    private final int indexMask;
    //就是上面的n，用来定位某个sequence到底转了多少圈，用来标识已被发布的sequence。
    //为什么不直接将sequence存入availableBuffer，因为这样sequence值会过大，很容易溢出
    private final int indexShift;

    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        indexShift = Util.log2(bufferSize);
        initialiseAvailableBuffer();
    }

    /**
     * 将availableBuffer都初始化为-1
     */
    private void initialiseAvailableBuffer() {
        for (int i = availableBuffer.length - 1; i != 0; i--) {
            setAvailableBufferValue(i, -1);
        }
        setAvailableBufferValue(0, -1);
    }

    /**
     * 发布某个sequence之前的都可以被消费了需要将availableBuffer上对应sequence下标的值设置为第几次用到这个槽
     * @param sequence
     */
    private void setAvailable(final long sequence) {
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    /**
     * 某个sequence右移indexShift，代表这个Sequence是第几次用到这个ringBuffer的某个槽，也就是这个sequence转了多少圈
     * @param sequence
     * @return
     */
    private int calculateAvailabilityFlag(final long sequence) {
        return (int) (sequence >>> indexShift);
    }

    /**
     * 定位ringBuffer上某个槽用于生产event，对2^n取模 = 对2^n -1
     * @param sequence
     * @return
     */
    private int calculateIndex(final long sequence) {
        return ((int) sequence) & indexMask;
    }

    /**
     * 通过Unsafe更新数组非volatile类型的值
     * 数组结构
     * --------------
     * *   数组头   * BASE
     * * reference1 * SCALE
     * * reference2 * SCALE
     * * reference3 * SCALE
     * --------------
     * @param index
     * @param flag
     */
    private void setAvailableBufferValue(int index, int flag) {
        long bufferAddress = (index * SCALE) + BASE;
        Platform.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    @Override
    public void claim(long sequence) {
        cursor.set(sequence);
    }

    /**
     * 用同样的方法计算给定的sequence，判断与availableBuffer对应下标的值是否相等，如果相等证明已被发布可以消费
     * @param sequence of the buffer to check
     * @return
     */
    @Override
    public boolean isAvailable(long sequence) {
        int index = calculateIndex(sequence);
        int flag = calculateAvailabilityFlag(sequence);
        long bufferAddress = (index * SCALE) + BASE;
        return Platform.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    /**
     * 获取最高的可消费sequence，减少获取次数
     * @param nextSequence      The sequence to start scanning from.
     * @param availableSequence The sequence to scan to.
     * @return
     */
    @Override
    public long getHighestPublishedSequence(long nextSequence, long availableSequence) {
        for (long sequence = nextSequence; sequence <= availableSequence; sequence++) {
            if (!isAvailable(sequence)) {
                return sequence - 1;
            }
        }
        return availableSequence;
    }

    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity) {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    /**
     * 与单一生产者验证原理类似
     * @param gatingSequences
     * @param requiredCapacity
     * @param cursorValue
     * @return
     */
    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue)
    {
        //下一位置加上所需容量减去整个bufferSize，如果为正数，那证明至少转了一圈，则需要检查gatingSequences（由消费者更新里面的Sequence值）以保证不覆盖还未被消费的
        //由于最多只能生产不大于整个bufferSize的Events。所以减去一个bufferSize与最小sequence相比较即可
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        //缓存
        long cachedGatingSequence = gatingSequenceCache.get();
        //缓存失效条件
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
        {
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);
            //空间不足
            if (wrapPoint > minSequence)
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public long remainingCapacity() {
        //与单一生产者的计算方法同理，不考虑并发
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        long produced = cursor.get();
        return getBufferSize() - (produced - consumed);
    }

    @Override
    public long next() {
        return next(1);
    }

    /**
     * 用于多个生产者抢占n个RingBuffer槽用于生产Event
     *
     * @param n
     * @return
     */
    @Override
    public long next(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do {
            //首先通过缓存判断空间是否足够
            current = cursor.get();
            next = current + n;

            long wrapPoint = next - bufferSize;
            long cachedGatingSequence = gatingSequenceCache.get();
            //如果缓存不满足
            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current) {
                //重新获取最小的
                long gatingSequence = Util.getMinimumSequence(gatingSequences, current);
                //如果空间不足，则唤醒消费者消费，并让出CPU
                if (wrapPoint > gatingSequence) {
                    waitStrategy.signalAllWhenBlocking();
                    LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
                    continue;
                }
                //重新设置缓存
                gatingSequenceCache.set(gatingSequence);
            } //如果空间足够，尝试CAS更新cursor，更新cursor成功代表成功获取n个槽，退出死循环
            else if (cursor.compareAndSet(current, next)) {
                break;
            }
        }
        while (true);
        //返回最新的cursor值
        return next;
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

        long current;
        long next;
        //尝试获取一次，若不成功，则抛InsufficientCapacityException
        do {
            current = cursor.get();
            next = current + n;

            if (!hasAvailableCapacity(gatingSequences, n, current)) {
                throw InsufficientCapacityException.INSTANCE;
            }
        }
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    @Override
    public void publish(long sequence) {
        setAvailable(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void publish(long lo, long hi) {
        for (long l = lo; l <= hi; l++) {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }
}
