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

import io.mycat.memory.MyCatMemory;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.row.StructType;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.sqlengine.mpp.OrderCol;
import io.mycat.sqlengine.mpp.RowDataPacketSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

public final class UnsafeExternalRowSorter {

    private final Logger logger = LoggerFactory.getLogger(UnsafeExternalSorter.class);

  private long numRowsInserted = 0;
  private final StructType schema;
  private final PrefixComputer prefixComputer;
  private final UnsafeExternalSorter sorter;
  private final  PrefixComparator prefixComparator;
  private final RecordComparator recordComparator;


  public abstract static class PrefixComputer {
    protected abstract long computePrefix(UnsafeRow row) throws UnsupportedEncodingException;
  }

  public UnsafeExternalRowSorter(DataNodeMemoryManager dataNodeMemoryManager,
                                 @Nonnull MyCatMemory myCatMemory,
                                 StructType schema,
                                 PrefixComparator prefixComparator,
                                 PrefixComputer prefixComputer,
                                 long pageSizeBytes,
                                 boolean canUseRadixSort,
                                 boolean enableSort) throws IOException {
    this.schema = schema;
    this.prefixComputer = prefixComputer;
      this.prefixComparator = prefixComparator;
      this.recordComparator =  new RowComparator(schema);
    sorter = UnsafeExternalSorter.create(
            dataNodeMemoryManager,
            myCatMemory.getBlockManager(),
           myCatMemory.getSerializerManager(),
            recordComparator,
      prefixComparator,
      myCatMemory.getConf().getSizeAsBytes("mycat.pointer.array.len","1K"),
      pageSizeBytes,
      canUseRadixSort,
      enableSort);
  }


  public void insertRow(UnsafeRow row) throws IOException {
    final long prefix = prefixComputer.computePrefix(row);

      sorter.insertRecord(
      row.getBaseObject(),
      row.getBaseOffset(),
      row.getSizeInBytes(),
      prefix);

    numRowsInserted++;
  }
    /**
     * Return total rows
     */
    public long getNumRowsInserted() {
        return numRowsInserted;
    }
  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsage() {
    return sorter.getPeakMemoryUsedBytes();
  }

  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    return sorter.getSortTimeNanos();
  }

  public void cleanupResources() {
      sorter.cleanupResources();
  }

  public Iterator<UnsafeRow> sort() throws IOException {
    try {
      final UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      if (!sortedIterator.hasNext()) {
        cleanupResources();
      }

      return new AbstractScalaRowIterator<UnsafeRow>() {

        private final int numFields = schema.length();
        private UnsafeRow row = new UnsafeRow(numFields);

        @Override
        public boolean hasNext() {
          return sortedIterator.hasNext();
        }

        @Override
        public UnsafeRow next() {
          try {
            sortedIterator.loadNext();
            row.pointTo(sortedIterator.getBaseObject(), sortedIterator.getBaseOffset(), sortedIterator.getRecordLength());
            if (!hasNext()) {
              UnsafeRow copy = row.copy(); // so that we don't have dangling pointers to freed page
              row = null; // so that we don't keep references to the base object
              cleanupResources();
              return copy;
            } else {
              return row;
            }
          } catch (IOException e) {
            cleanupResources();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            Platform.throwException(e);
          }
          throw new RuntimeException("Exception should have been re-thrown in next()");
        }

        @Override
        public void remove() {

        }
      };
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }


  public UnsafeSorterIterator getRowUnsafeSorterIterator() throws IOException{
    return sorter.getSortedIterator();
  }


    public Iterator<UnsafeRow> mergerSort(List<UnsafeSorterIterator> list) throws IOException {

        UnsafeRowsMerger unsafeRowsMerger = new UnsafeRowsMerger(recordComparator,prefixComparator,list.size());

        for (int i = 0; i <list.size() ; i++) {
            unsafeRowsMerger.addSpillIfNotEmpty(list.get(i));
        }

        try {
            final UnsafeSorterIterator sortedIterator = unsafeRowsMerger.getSortedIterator();

            if (!sortedIterator.hasNext()) {
                cleanupResources();
            }

            return new AbstractScalaRowIterator<UnsafeRow>() {

                private final int numFields = schema.length();
                private UnsafeRow row = new UnsafeRow(numFields);

                @Override
                public boolean hasNext() {
                    return sortedIterator.hasNext();
                }

                @Override
                public UnsafeRow next() {
                    try {
                        sortedIterator.loadNext();
                        row.pointTo(
                                sortedIterator.getBaseObject(),
                                sortedIterator.getBaseOffset(),
                                sortedIterator.getRecordLength());
                        if (!hasNext()) {
                            UnsafeRow copy = row.copy(); // so that we don't have dangling pointers to freed page
                            row = null; // so that we don't keep references to the base object
                            cleanupResources();
                            return copy;
                        } else {
                            return row;
                        }
                    } catch (IOException e) {
                        cleanupResources();
                        // Scala iterators don't declare any checked exceptions, so we need to use this hack
                        // to re-throw the exception:
                        Platform.throwException(e);
                    }
                    throw new RuntimeException("Exception should have been re-thrown in next()");
                }

                @Override
                public void remove() {

                }
            };
        } catch (IOException e) {
            cleanupResources();
            throw e;
        }
    }


  public Iterator<UnsafeRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {

    while (inputIterator.hasNext()) {
      insertRow(inputIterator.next());
    }

    return sort();
  }



  private static final class RowComparator extends RecordComparator {
    private final int numFields;
    private final UnsafeRow row1;
    private final UnsafeRow row2;
    private final StructType schema;

    RowComparator(StructType schema) {

      assert schema.length()>=0;

      this.schema = schema;
      this.numFields = schema.length();
      this.row1 = new UnsafeRow(numFields);
      this.row2 = new UnsafeRow(numFields);
    }

    @Override
    public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
      OrderCol[] orderCols =  schema.getOrderCols();

      if(orderCols == null){
          return 0;
      }

      /**取出一行数据*/
      row1.pointTo(baseObj1, baseOff1, -1);
      row2.pointTo(baseObj2, baseOff2, -1);
      int cmp = 0;
      int len = orderCols.length;

      int type = OrderCol.COL_ORDER_TYPE_ASC; /**升序*/

      for (int i = 0; i < len; i++) {
        int colIndex = orderCols[i].colMeta.colIndex;
        /**取出一行数据中的列值，进行大小比对*/
        byte[] left = null;
        byte[] right = null;



          if(!row1.isNullAt(colIndex)) {
              left = row1.getBinary(colIndex);
          }else {
              left = new byte[1];
              left[0] = UnsafeRow.NULL_MARK;
          }


          if(!row2.isNullAt(colIndex)) {
              right = row2.getBinary(colIndex);
          }else {
              right = new byte[1];
              right[0] = UnsafeRow.NULL_MARK;
          }

        if (orderCols[i].orderType == type) {
          cmp = RowDataPacketSorter.compareObject(left, right, orderCols[i]);
        } else {
          cmp = RowDataPacketSorter.compareObject(right, left, orderCols[i]);
        }
        if (cmp != 0)
          return cmp;
      }
      return cmp;
    }
  }
}
