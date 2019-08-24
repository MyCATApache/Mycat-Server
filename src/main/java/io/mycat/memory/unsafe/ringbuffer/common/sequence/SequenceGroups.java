package io.mycat.memory.unsafe.ringbuffer.common.sequence;

import io.mycat.memory.unsafe.ringbuffer.common.Cursored;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.Arrays.copyOf;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/25
 */
public class SequenceGroups {
    /**
     * 原子添加sequences
     *
     * @param holder 原子更新的域所属的类对象
     * @param updater 原子更新的域对象
     * @param cursor 定位
     * @param sequencesToAdd 要添加的sequences
     * @param <T>
     */
    public static <T> void addSequences(
            final T holder,
            final AtomicReferenceFieldUpdater<T, Sequence[]> updater,
            final Cursored cursor,
            final Sequence... sequencesToAdd)
    {
        long cursorSequence;
        Sequence[] updatedSequences;
        Sequence[] currentSequences;
        //在更新成功之前，一直重新读取currentSequences，扩充为添加所有sequence之后的updatedSequences
        do
        {
            currentSequences = updater.get(holder);
            updatedSequences = copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);
            cursorSequence = cursor.getCursor();

            int index = currentSequences.length;
            //将新的sequences的值设置为cursorSequence
            for (Sequence sequence : sequencesToAdd)
            {
                sequence.set(cursorSequence);
                updatedSequences[index++] = sequence;
            }
        }
        while (!updater.compareAndSet(holder, currentSequences, updatedSequences));

        cursorSequence = cursor.getCursor();
        for (Sequence sequence : sequencesToAdd)
        {
            sequence.set(cursorSequence);
        }
    }

    /**
     * 原子移除某个指定的sequence
     *
     * @param holder 原子更新的域所属的类对象
     * @param sequenceUpdater 原子更新的域对象
     * @param sequence 要移除的sequence
     * @param <T>
     * @return
     */
    public static <T> boolean removeSequence(
            final T holder,
            final AtomicReferenceFieldUpdater<T, Sequence[]> sequenceUpdater,
            final Sequence sequence)
    {
        int numToRemove;
        Sequence[] oldSequences;
        Sequence[] newSequences;

        do
        {
            oldSequences = sequenceUpdater.get(holder);

            numToRemove = countMatching(oldSequences, sequence);

            if (0 == numToRemove)
            {
                break;
            }

            final int oldSize = oldSequences.length;
            newSequences = new Sequence[oldSize - numToRemove];

            for (int i = 0, pos = 0; i < oldSize; i++)
            {
                final Sequence testSequence = oldSequences[i];
                if (sequence != testSequence)
                {
                    newSequences[pos++] = testSequence;
                }
            }
        }
        while (!sequenceUpdater.compareAndSet(holder, oldSequences, newSequences));

        return numToRemove != 0;
    }

    private static <T> int countMatching(T[] values, final T toMatch)
    {
        int numToRemove = 0;
        for (T value : values)
        {
            if (value == toMatch) // Specifically uses identity
            {
                numToRemove++;
            }
        }
        return numToRemove;
    }
}
