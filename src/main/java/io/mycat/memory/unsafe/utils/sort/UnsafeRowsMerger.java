package io.mycat.memory.unsafe.utils.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by zagnix on 2016/6/25.
 */
public final class UnsafeRowsMerger {
    private int numRecords = 0;
    private final PriorityQueue<UnsafeSorterIterator> priorityQueue;

    UnsafeRowsMerger(
            final RecordComparator recordComparator,
            final PrefixComparator prefixComparator,
            final int numSpills) {

        final Comparator<UnsafeSorterIterator> comparator = new Comparator<UnsafeSorterIterator>() {
            @Override
            public int compare(UnsafeSorterIterator left, UnsafeSorterIterator right) {
                final int prefixComparisonResult = prefixComparator.compare(left.getKeyPrefix(), right.getKeyPrefix());
                if (prefixComparisonResult == 0) {
                    return recordComparator.compare(
                            left.getBaseObject(), left.getBaseOffset(),
                            right.getBaseObject(), right.getBaseOffset());
                } else {
                    return prefixComparisonResult;
                }
            }
        };

        /**
         * 使用优先级队列实现多个Spill File 合并排序,并且支持已经排序内存记录
         * 重新写入一个排序文件中。
         */
        priorityQueue = new PriorityQueue<UnsafeSorterIterator>(numSpills,comparator);
    }

    /**
     * Add an UnsafeSorterIterator to this merger
     *
     */
    public void addSpillIfNotEmpty(UnsafeSorterIterator iterator) throws IOException {
        /**
         * 添加迭代器到priorityQueue中
         */
        if (iterator.hasNext()) {
            iterator.loadNext();
            priorityQueue.add(iterator);
            numRecords += iterator.getNumRecords();
        }
    }

    public int getNumRecords() {
        return numRecords;
    }

    public UnsafeSorterIterator getSortedIterator() throws IOException {
        return new UnsafeSorterIterator() {
            private UnsafeSorterIterator spillReader;

            @Override
            public int getNumRecords() {
                return numRecords;
            }

            @Override
            public boolean hasNext() {
                return !priorityQueue.isEmpty() || (spillReader != null && spillReader.hasNext());
            }

            @Override
            public void loadNext() throws IOException {
                if (spillReader != null) {
                    if (spillReader.hasNext()) {
                        spillReader.loadNext();
                        priorityQueue.add(spillReader);
                    }
                }
                spillReader = priorityQueue.remove();
            }

            @Override
            public Object getBaseObject() { return spillReader.getBaseObject(); }

            @Override
            public long getBaseOffset() { return spillReader.getBaseOffset(); }

            @Override
            public int getRecordLength() { return spillReader.getRecordLength(); }

            @Override
            public long getKeyPrefix() { return spillReader.getKeyPrefix(); }
        };
    }
}
