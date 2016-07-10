/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mycat.memory.unsafe.utils.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

final class UnsafeSorterSpillMerger {

  private int numRecords = 0;
  private final PriorityQueue<UnsafeSorterIterator> priorityQueue;

  UnsafeSorterSpillMerger(
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
  public void addSpillIfNotEmpty(UnsafeSorterIterator spillReader) throws IOException {
    /**
     * 添加迭代器到priorityQueue中
     */
    if (spillReader.hasNext()) {
      // We only add the spillReader to the priorityQueue if it is not empty. We do this to
      // make sure the hasNext method of UnsafeSorterIterator returned by getSortedIterator
      // does not return wrong result because hasNext will returns true
      // at least priorityQueue.size() times. If we allow n spillReaders in the
      // priorityQueue, we will have n extra empty records in the result of UnsafeSorterIterator.

      spillReader.loadNext();
      priorityQueue.add(spillReader);
      numRecords += spillReader.getNumRecords();
    }
  }

  /**
   * 非常重要的一个排序迭代器
   * @return
   * @throws IOException
     */
  public UnsafeSorterIterator getSortedIterator() throws IOException {
    return new UnsafeSorterIterator() {
      /**
       * 当前迭代器
       */
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
             /**
             *添加一个完整迭代器集合给优先级队列，
             *优先级队列为根据比较器自动调整想要的数据大小
             * 每次都将spillReader添加到队列中进行新的调整
             * 最后得到最小的元素，为出优先级队列做准备
             */
            priorityQueue.add(spillReader);
          }
        }

        /**
         * 出队列，当前spillreader最小的元素出优先级队列
         */
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
