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

package io.mycat.memory.unsafe.map;

import io.mycat.memory.MyCatMemory;
import io.mycat.memory.unsafe.KVIterator;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.memory.mm.MemoryManager;
import io.mycat.memory.unsafe.row.BufferHolder;
import io.mycat.memory.unsafe.row.StructType;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.memory.unsafe.row.UnsafeRowWriter;
import io.mycat.memory.unsafe.utils.BytesTools;
import io.mycat.sqlengine.mpp.ColMeta;
import io.mycat.sqlengine.mpp.OrderCol;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;


import java.io.IOException;
import java.util.*;

/**
 * Created by zagnix on 2016/6/4.
 */
public class UnsafeFixedWidthAggregationMapSuite {
  private StructType groupKeySchema ;
  private StructType aggBufferSchema;
  private UnsafeRow emptyAggregationBuffer;
  private long PAGE_SIZE_BYTES  = 1L << 20;

  private final Random rand = new Random(42);

  private static Logger LOGGER = Logger.getLogger(UnsafeFixedWidthAggregationMapSuite.class);
  @Test
  public void testAggregateMap() throws NoSuchFieldException, IllegalAccessException, IOException {
    /**
     * 创造上文环境
     */
    MyCatMemory myCatMemory = new MyCatMemory();
    MemoryManager memoryManager = myCatMemory.getResultMergeMemoryManager();
    DataNodeMemoryManager dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager, Thread.currentThread().getId());

      /**
       * 构造数据字段group key
       */

    int fieldCount = 2;
    ColMeta colMeta = null;
    Map<String,ColMeta> colMetaMap = new HashMap<String,ColMeta>(fieldCount);
    colMeta = new ColMeta(0,ColMeta.COL_TYPE_STRING);
    colMetaMap.put("id",colMeta);
    colMeta = new ColMeta(1,ColMeta.COL_TYPE_STRING);
    colMetaMap.put("name",colMeta);

    OrderCol[] orderCols = new OrderCol[1];
    OrderCol orderCol = new OrderCol(colMetaMap.get("id"),OrderCol.COL_ORDER_TYPE_DESC);
    orderCols[0] = orderCol;

    groupKeySchema = new StructType(colMetaMap,fieldCount);
    groupKeySchema.setOrderCols(orderCols);


  /**
   * 构造数据字段value key
   */
    fieldCount = 4;
    colMeta = null;
    colMetaMap = new HashMap<String,ColMeta>(fieldCount);
    colMeta = new ColMeta(0,ColMeta.COL_TYPE_STRING);
    colMetaMap.put("id",colMeta);
    colMeta = new ColMeta(1,ColMeta.COL_TYPE_STRING);
    colMetaMap.put("name",colMeta);
    colMeta = new ColMeta(2,ColMeta.COL_TYPE_INT);
    colMetaMap.put("age",colMeta);

    colMeta = new ColMeta(3,ColMeta.COL_TYPE_LONGLONG);
    colMetaMap.put("score",colMeta);


    orderCols = new OrderCol[1];
    orderCol = new OrderCol(colMetaMap.get("id"),OrderCol.COL_ORDER_TYPE_DESC);
    orderCols[0] = orderCol;

    aggBufferSchema = new StructType(colMetaMap,fieldCount);
    aggBufferSchema.setOrderCols(orderCols);

    /**
     *emtpy Row value
     */
    BufferHolder bufferHolder ;
    emptyAggregationBuffer = new UnsafeRow(4);
    bufferHolder = new BufferHolder(emptyAggregationBuffer,0);
    UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(bufferHolder,4);
    bufferHolder.reset();
    String value = "o";
    unsafeRowWriter.write(0,value.getBytes());
    unsafeRowWriter.write(1,value.getBytes());
    emptyAggregationBuffer.setInt(2,0);
    emptyAggregationBuffer.setLong(3,0);
    emptyAggregationBuffer.setTotalSize(bufferHolder.totalSize());


    UnsafeFixedWidthAggregationMap map = new UnsafeFixedWidthAggregationMap(
            emptyAggregationBuffer,
            aggBufferSchema,
            groupKeySchema,
            dataNodeMemoryManager,
            2*1024,
            PAGE_SIZE_BYTES,
            true);


      /**
       * 造数据
       */

    int i;

    List<UnsafeRow> rows = new  ArrayList<UnsafeRow>();
    for ( i = 0; i < 100000; i++) {
      /**
       * key
       */
      UnsafeRow groupKey = new UnsafeRow(2);
      bufferHolder = new BufferHolder(groupKey,0);
      unsafeRowWriter = new UnsafeRowWriter(bufferHolder,2);
      bufferHolder.reset();

      unsafeRowWriter.write(0, BytesTools.toBytes(rand.nextInt(10000000)));
      unsafeRowWriter.write(1,BytesTools.toBytes(rand.nextInt(10000000)));

      groupKey.setTotalSize(bufferHolder.totalSize());

      UnsafeRow valueKey = new UnsafeRow(4);
      bufferHolder = new BufferHolder(valueKey,0);
      unsafeRowWriter = new UnsafeRowWriter(bufferHolder,4);
      bufferHolder.reset();

      unsafeRowWriter.write(0, BytesTools.toBytes(rand.nextInt(10)));
      unsafeRowWriter.write(1,BytesTools.toBytes(rand.nextInt(10)));
      valueKey.setInt(2,i);
      valueKey.setLong(3,1);
      valueKey.setTotalSize(bufferHolder.totalSize());

      if(map.find(groupKey)){
          UnsafeRow rs = map.getAggregationBuffer(groupKey);
          rs.setLong(3,i+valueKey.getLong(3));
          rs.setInt(2,100+valueKey.getInt(2));
      }else {
        map.put(groupKey,valueKey);
      }
      rows.add(valueKey);
    }


    KVIterator<UnsafeRow,UnsafeRow> iter = map.iterator();
    int j = 0;
    while (iter.next()){
      Assert.assertEquals(j,iter.getValue().getInt(2));
      j++;
      iter.getValue().setInt(2,5000000);
      iter.getValue().setLong(3,600000);
    }

    Assert.assertEquals(rows.size(),j);
    int k = 0;
    KVIterator<UnsafeRow,UnsafeRow> iter1 = map.iterator();
    while (iter1.next()){
      k++;
     // LOGGER.error("(" + BytesTools.toInt(iter1.getKey().getBinary(0)) + "," +
      //       iter1.getValue().getInt(2) +"," +iter1.getValue().getLong(3)+")");

      Assert.assertEquals(5000000,iter1.getValue().getInt(2));
      Assert.assertEquals(600000,iter1.getValue().getLong(3));
    }

    Assert.assertEquals(j,k);

    map.free();

  }
@Test
public void  testWithMemoryLeakDetection() throws IOException, NoSuchFieldException, IllegalAccessException {
  MyCatMemory myCatMemory = new MyCatMemory();
  MemoryManager memoryManager = myCatMemory.getResultMergeMemoryManager();
  DataNodeMemoryManager dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager,
          Thread.currentThread().getId());
    int fieldCount = 3;
    ColMeta colMeta = null;
    Map<String,ColMeta> colMetaMap = new HashMap<String,ColMeta>(fieldCount);
    colMeta = new ColMeta(0,ColMeta.COL_TYPE_STRING);
    colMetaMap.put("id",colMeta);
    colMeta = new ColMeta(1,ColMeta.COL_TYPE_STRING);
    colMetaMap.put("name",colMeta);
    colMeta = new ColMeta(2,ColMeta.COL_TYPE_STRING);
    colMetaMap.put("age",colMeta);


    OrderCol[] orderCols = new OrderCol[1];
    OrderCol orderCol = new OrderCol(colMetaMap.get("id"),OrderCol.COL_ORDER_TYPE_DESC);
    orderCols[0] = orderCol;

    groupKeySchema = new StructType(colMetaMap,fieldCount);
    groupKeySchema.setOrderCols(orderCols);



   fieldCount = 3;
   colMeta = null;
   colMetaMap = new HashMap<String,ColMeta>(fieldCount);
   colMeta = new ColMeta(0,ColMeta.COL_TYPE_LONGLONG);
   colMetaMap.put("age",colMeta);
   colMeta = new ColMeta(1,ColMeta.COL_TYPE_LONGLONG);
   colMetaMap.put("age1",colMeta);
   colMeta = new ColMeta(2,ColMeta.COL_TYPE_STRING);
   colMetaMap.put("name",colMeta);

   orderCols = new OrderCol[1];
   orderCol = new OrderCol(colMetaMap.get("id"),OrderCol.COL_ORDER_TYPE_DESC);
   orderCols[0] = orderCol;

  aggBufferSchema = new StructType(colMetaMap,fieldCount);
  aggBufferSchema.setOrderCols(orderCols);

  /**
   * value
   */
  BufferHolder bufferHolder ;
  emptyAggregationBuffer = new UnsafeRow(3);
  bufferHolder = new BufferHolder(emptyAggregationBuffer,0);
  UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(bufferHolder,3);
  bufferHolder.reset();
  String value = "ok,hello";
  emptyAggregationBuffer.setLong(0,0);
  emptyAggregationBuffer.setLong(1,0);
  unsafeRowWriter.write(2,value.getBytes());
  emptyAggregationBuffer.setTotalSize(bufferHolder.totalSize());

  UnsafeFixedWidthAggregationMap map = new UnsafeFixedWidthAggregationMap(
          emptyAggregationBuffer,
          aggBufferSchema,
          groupKeySchema,
          dataNodeMemoryManager,
          2*1024,
          PAGE_SIZE_BYTES,
          false
  );


  int i;

  List<UnsafeRow> rows = new  ArrayList<UnsafeRow>();
  for ( i = 0; i < 1000; i++) {
    String line = "testUnsafeRow" + i;
    /**
     * key
     */
    UnsafeRow groupKey = new UnsafeRow(3);
    bufferHolder = new BufferHolder(groupKey,0);
    unsafeRowWriter = new UnsafeRowWriter(bufferHolder,3);
    bufferHolder.reset();

    final byte[] key = getRandomByteArray(rand.nextInt(8));
    String age = "5"+i;
    unsafeRowWriter.write(0,key);
    unsafeRowWriter.write(1,line.getBytes());
    unsafeRowWriter.write(2,age.getBytes());
    groupKey.setTotalSize(bufferHolder.totalSize());

    map.getAggregationBuffer(groupKey);

    rows.add(groupKey);
  }

  Assert.assertEquals(i ,rows.size() );



  UnsafeRow row = rows.get(12);
  UnsafeRow rs = map.getAggregationBuffer(row);
  rs.setLong(0,12);
  rs = map.getAggregationBuffer(row);
  Assert.assertEquals(12,rs.getLong(0));

  map.free();

  }

  private byte[] getRandomByteArray(int numWords) {
    Assert.assertTrue(numWords >= 0);
    final int lengthInBytes = numWords * 8;
    final byte[] bytes = new byte[lengthInBytes];
    rand.nextBytes(bytes);
    return bytes;
  }

}
