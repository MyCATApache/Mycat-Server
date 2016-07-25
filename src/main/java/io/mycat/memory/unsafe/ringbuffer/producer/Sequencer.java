package io.mycat.memory.unsafe.ringbuffer.producer;

import io.mycat.memory.unsafe.ringbuffer.common.Cursored;
import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.common.Sequenced;
import io.mycat.memory.unsafe.ringbuffer.common.barrier.SequenceBarrier;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/23
 */
public interface Sequencer extends Cursored,Sequenced{
    /**
     * -1 为 sequence的起始值
     */
    long INITIAL_CURSOR_VALUE = -1L;

    /**
     * 申请一个特殊的Sequence，只有设定特殊起始值的ringBuffer时才会使用
     *
     * @param sequence The sequence to initialise too.
     */
    void claim(long sequence);

    /**
     * 非阻塞，验证一个sequence是否已经被published并且可以消费
     *
     * @param sequence of the buffer to check
     * @return true if the sequence is available for use, false if not
     */
    boolean isAvailable(long sequence);

    /**
     * 将这些sequence加入到需要跟踪处理的gatingSequences中
     *
     * @param gatingSequences The sequences to add.
     */
    void addGatingSequences(Sequence... gatingSequences);

    /**
     * 移除某个sequence
     *
     * @param sequence to be removed.
     * @return <tt>true</tt> if this sequence was found, <tt>false</tt> otherwise.
     */
    boolean removeGatingSequence(Sequence sequence);

    /**
     * 给定一串需要跟踪的sequence，创建SequenceBarrier
     * SequenceBarrier是用来给多消费者确定消费位置是否可以消费用的
     *
     * @param sequencesToTrack
     * @return A sequence barrier that will track the specified sequences.
     * @see SequenceBarrier
     */
    SequenceBarrier newBarrier(Sequence... sequencesToTrack);

    /**
     * 获取这个ringBuffer的gatingSequences中最小的一个sequence
     *
     * @return The minimum gating sequence or the cursor sequence if
     */
    long getMinimumSequence();

    /**
     * 获取最高可以读取的Sequence
     *
     * @param nextSequence      The sequence to start scanning from.
     * @param availableSequence The sequence to scan to.
     * @return The highest value that can be safely read, will be at least <code>nextSequence - 1</code>.
     */
    long getHighestPublishedSequence(long nextSequence, long availableSequence);
    /**
     * 并没有什么用，不实现，注释掉
     */
//    <T> EventPoller<T> newPoller(DataProvider<T> provider, Sequence... gatingSequences);
}
