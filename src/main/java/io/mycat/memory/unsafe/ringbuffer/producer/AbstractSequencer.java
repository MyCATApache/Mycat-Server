package io.mycat.memory.unsafe.ringbuffer.producer;

import io.mycat.memory.unsafe.ringbuffer.common.sequence.Sequence;
import io.mycat.memory.unsafe.ringbuffer.common.barrier.SequenceBarrier;
import io.mycat.memory.unsafe.ringbuffer.common.sequence.SequenceGroups;
import io.mycat.memory.unsafe.ringbuffer.common.waitStrategy.WaitStrategy;
import io.mycat.memory.unsafe.ringbuffer.exception.InsufficientCapacityException;
import io.mycat.memory.unsafe.ringbuffer.utils.Util;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/24
 */
public abstract class AbstractSequencer implements Sequencer {

    private static final AtomicReferenceFieldUpdater<AbstractSequencer, Sequence[]> SEQUENCE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(AbstractSequencer.class, Sequence[].class, "gatingSequences");

    protected final int bufferSize;
    protected final WaitStrategy waitStrategy;
    protected final Sequence cursor = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    protected volatile Sequence[] gatingSequences = new Sequence[0];

    public AbstractSequencer(int bufferSize, WaitStrategy waitStrategy)
    {
        if (bufferSize < 1)
        {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1)
        {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;
    }

    @Override
    public final long getCursor()
    {
        return cursor.get();
    }


    @Override
    public final int getBufferSize()
    {
        return bufferSize;
    }

    @Override
    public void addGatingSequences(Sequence... gatingSequences) {
        SequenceGroups.addSequences(this, SEQUENCE_UPDATER, this, gatingSequences);
    }

    @Override
    public boolean removeGatingSequence(Sequence sequence) {
        return SequenceGroups.removeSequence(this, SEQUENCE_UPDATER, sequence);
    }

    @Override
    public long getMinimumSequence() {
        return Util.getMinimumSequence(gatingSequences, cursor.get());
    }

    public SequenceBarrier newBarrier(Sequence... sequencesToTrack)
    {
        return null;
        //TODO 完成SequenceBarrier
//        return new ProcessingSequenceBarrier(this, waitStrategy, cursor, sequencesToTrack);
    }
}
