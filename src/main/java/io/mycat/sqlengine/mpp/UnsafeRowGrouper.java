/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.sqlengine.mpp;

import io.mycat.MycatServer;
import io.mycat.memory.MyCatMemory;
import io.mycat.memory.unsafe.KVIterator;
import io.mycat.memory.unsafe.map.UnsafeFixedWidthAggregationMap;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.memory.mm.MemoryManager;
import io.mycat.memory.unsafe.row.BufferHolder;
import io.mycat.memory.unsafe.row.StructType;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.memory.unsafe.row.UnsafeRowWriter;

import io.mycat.memory.unsafe.utils.BytesTools;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import io.mycat.memory.unsafe.utils.sort.UnsafeExternalRowSorter;
import io.mycat.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by zagnix on 2016/6/26.
 *
 * implement group function select a,count(*),sum(*) from A group by a
 *
 */
public class UnsafeRowGrouper {
	private static final Logger logger = LoggerFactory.getLogger(UnsafeRowGrouper.class);

	private UnsafeFixedWidthAggregationMap aggregationMap = null;
	private final Map<String, ColMeta> columToIndx;
	private final MergeCol[] mergCols;
        private String[] sortColumnsByIndex = null;
 	private final String[] columns;
	private boolean isMergAvg=false;
	private HavingCols havingCols;
	private UnsafeRow groupKey = null;
	private UnsafeRow valueKey = null;
	private BufferHolder bufferHolder = null;
	private UnsafeRowWriter unsafeRowWriter = null;
	private final int groupKeyfieldCount;
	private final int valuefieldCount;
	private StructType groupKeySchema ;
	private StructType aggBufferSchema;
	private UnsafeRow emptyAggregationBuffer;
	private final MyCatMemory myCatMemory;
	private final MemoryManager memoryManager;
	private final MycatPropertyConf conf;

	public UnsafeRowGrouper(Map<String,ColMeta> columToIndx,String[] columns, MergeCol[] mergCols, HavingCols havingCols) {
		super();
		assert columns!=null;
		assert columToIndx!=null;
		assert mergCols !=null;
		this.columToIndx = columToIndx;
		this.columns = columns;
		this.mergCols = mergCols;
		this.havingCols = havingCols;
                this.sortColumnsByIndex =  columns !=null ? toSortColumnsByIndex(columns,columToIndx):null;
		this.groupKeyfieldCount = columns != null?columns.length:0;
		this.valuefieldCount = columToIndx != null?columToIndx.size():0;
		this.myCatMemory = MycatServer.getInstance().getMyCatMemory();
		this.memoryManager = myCatMemory.getResultMergeMemoryManager();
		this.conf = myCatMemory.getConf();

		logger.debug("columToIndx :" + (columToIndx != null ? columToIndx.toString():"null"));

		initGroupKey();
		initEmptyValueKey();

		DataNodeMemoryManager dataNodeMemoryManager =
				new DataNodeMemoryManager(memoryManager,Thread.currentThread().getId());

		aggregationMap = new UnsafeFixedWidthAggregationMap(
				emptyAggregationBuffer,
				aggBufferSchema,
				groupKeySchema,
				dataNodeMemoryManager,
				2*1024,
				conf.getSizeAsBytes("mycat.buffer.pageSize", "1m"),
				false);
	}

    private String[] toSortColumnsByIndex(String[] columns, Map<String, ColMeta> columToIndx) {

        Map<String,Integer> map = new HashMap<String,Integer>();

        ColMeta curColMeta;
        for (int i = 0; i < columns.length; i++) {
            curColMeta = columToIndx.get(columns[i].toUpperCase());
            if (curColMeta == null) {
                throw new IllegalArgumentException(
                        "all columns in group by clause should be in the selected column list.!"
                                + columns[i]);
            }
            map.put(columns[i],curColMeta.colIndex);
        }


        String[] sortColumnsByIndex = new String[map.size()];

        List<Map.Entry<String, Integer>> entryList = new ArrayList<
                Map.Entry<String, Integer>>(
                map.entrySet());

        Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        Iterator<Map.Entry<String, Integer>> iter = entryList.iterator();
        Map.Entry<String, Integer> tmpEntry = null;

        int index = 0;

        while (iter.hasNext()) {
            tmpEntry = iter.next();
            sortColumnsByIndex[index++] = tmpEntry.getKey();
        }

        return sortColumnsByIndex;
    }

    private void initGroupKey(){
		/**
		 * 构造groupKey
		 */
		Map<String,ColMeta> groupcolMetaMap = new HashMap<String,ColMeta>(this.groupKeyfieldCount);

		groupKey = new UnsafeRow(this.groupKeyfieldCount);
		bufferHolder = new BufferHolder(groupKey,0);
		unsafeRowWriter = new UnsafeRowWriter(bufferHolder,this.groupKeyfieldCount);
		bufferHolder.reset();

		ColMeta curColMeta = null;

		for (int i = 0; i < this.groupKeyfieldCount; i++) {
			curColMeta = this.columToIndx.get(sortColumnsByIndex[i].toUpperCase());
			groupcolMetaMap.put(sortColumnsByIndex[i],curColMeta);


				switch (curColMeta.colType) {
					case ColMeta.COL_TYPE_BIT:
						groupKey.setByte(i, (byte) 0);
						break;
					case ColMeta.COL_TYPE_INT:
					case ColMeta.COL_TYPE_INT24:
					case ColMeta.COL_TYPE_LONG:
						groupKey.setInt(i, 0);
						break;
					case ColMeta.COL_TYPE_SHORT:
						groupKey.setShort(i, (short) 0);
						break;
					case ColMeta.COL_TYPE_FLOAT:
						groupKey.setFloat(i, 0);
						break;
					case ColMeta.COL_TYPE_DOUBLE:
					case ColMeta.COL_TYPE_NEWDECIMAL:
						groupKey.setDouble(i, 0);
						break;
					case ColMeta.COL_TYPE_LONGLONG:
						groupKey.setLong(i, 0);
						break;
					default:
						unsafeRowWriter.write(i, "init".getBytes());
						break;
				}

		}
		groupKey.setTotalSize(bufferHolder.totalSize());

		groupKeySchema = new StructType(groupcolMetaMap,this.groupKeyfieldCount);
		groupKeySchema.setOrderCols(null);
	}

	private void initEmptyValueKey(){
		/**
		 * 构造valuerow
		 */
		emptyAggregationBuffer = new UnsafeRow(this.valuefieldCount);
		bufferHolder = new BufferHolder(emptyAggregationBuffer,0);
		unsafeRowWriter = new UnsafeRowWriter(bufferHolder,this.valuefieldCount);
		bufferHolder.reset();

		ColMeta curColMeta = null;
		for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
            curColMeta = fieldEntry.getValue();

				switch (curColMeta.colType) {
					case ColMeta.COL_TYPE_BIT:
						emptyAggregationBuffer.setByte(curColMeta.colIndex, (byte) 0);
						break;
					case ColMeta.COL_TYPE_INT:
					case ColMeta.COL_TYPE_INT24:
					case ColMeta.COL_TYPE_LONG:
						emptyAggregationBuffer.setInt(curColMeta.colIndex, 0);
						break;
					case ColMeta.COL_TYPE_SHORT:
						emptyAggregationBuffer.setShort(curColMeta.colIndex, (short) 0);
						break;
					case ColMeta.COL_TYPE_LONGLONG:
						emptyAggregationBuffer.setLong(curColMeta.colIndex, 0);
						break;
					case ColMeta.COL_TYPE_FLOAT:
						emptyAggregationBuffer.setFloat(curColMeta.colIndex, 0);
						break;
					case ColMeta.COL_TYPE_DOUBLE:
					case ColMeta.COL_TYPE_NEWDECIMAL:
						emptyAggregationBuffer.setDouble(curColMeta.colIndex, 0);
						break;
					default:
						unsafeRowWriter.write(curColMeta.colIndex, "init".getBytes());
						break;
				}

		}

		emptyAggregationBuffer.setTotalSize(bufferHolder.totalSize());
		aggBufferSchema = new StructType(columToIndx,this.valuefieldCount);
		aggBufferSchema.setOrderCols(null);
	}


	public Iterator<UnsafeRow> getResult(@Nonnull UnsafeExternalRowSorter sorter) throws IOException {
		KVIterator<UnsafeRow,UnsafeRow> iter = aggregationMap.iterator();
        /**
         * 求平均值
         */
		if (isMergeAvg() && !isMergAvg){
			try {
				while (iter.next()){
					mergAvg(iter.getValue());
				}
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
			isMergAvg = true;
		}
        /**
         * group having
         */
        if (havingCols !=null){
            filterHaving(sorter);
        }else{

            /**
             * KVIterator<K,V> ==>Iterator<V>
             */
            insertValue(sorter);
        }
		return sorter.sort();
	}


    public UnsafeRow getAllBinaryRow(UnsafeRow row) throws UnsupportedEncodingException {

        UnsafeRow value = new UnsafeRow( this.valuefieldCount);
        bufferHolder = new BufferHolder(value,0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.valuefieldCount);
        bufferHolder.reset();
        ColMeta curColMeta = null;

        for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
			curColMeta = fieldEntry.getValue();

			if (!row.isNullAt(curColMeta.colIndex)) {
				switch (curColMeta.colType) {
					case ColMeta.COL_TYPE_BIT:
						unsafeRowWriter.write(curColMeta.colIndex, row.getByte(curColMeta.colIndex));
						break;
					case ColMeta.COL_TYPE_INT:
					case ColMeta.COL_TYPE_LONG:
					case ColMeta.COL_TYPE_INT24:
						unsafeRowWriter.write(curColMeta.colIndex,
								BytesTools.int2Bytes(row.getInt(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_SHORT:
						unsafeRowWriter.write(curColMeta.colIndex,
								BytesTools.short2Bytes(row.getShort(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_LONGLONG:
						unsafeRowWriter.write(curColMeta.colIndex,
								BytesTools.long2Bytes(row.getLong(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_FLOAT:
						unsafeRowWriter.write(curColMeta.colIndex,
								BytesTools.float2Bytes(row.getFloat(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_DOUBLE:
					case ColMeta.COL_TYPE_NEWDECIMAL:
						unsafeRowWriter.write(curColMeta.colIndex,
								BytesTools.double2Bytes(row.getDouble(curColMeta.colIndex)));
						break;
					default:
						unsafeRowWriter.write(curColMeta.colIndex,
								row.getBinary(curColMeta.colIndex));
						break;
				}
			 }else {
				unsafeRowWriter.setNullAt(curColMeta.colIndex);
			}
			}

        value.setTotalSize(bufferHolder.totalSize());
        return value;
    }

    private void insertValue(@Nonnull UnsafeExternalRowSorter sorter){
            KVIterator<UnsafeRow,UnsafeRow> it = aggregationMap.iterator();
            try {
                while (it.next()){
                    UnsafeRow row = getAllBinaryRow(it.getValue());
                    sorter.insertRow(row);
                }
            } catch (IOException e) {
               logger.error(e.getMessage());
            }
    }

	private void filterHaving(@Nonnull UnsafeExternalRowSorter sorter){

        if (havingCols.getColMeta() == null || aggregationMap == null) {
			return;
		}
		KVIterator<UnsafeRow,UnsafeRow> it = aggregationMap.iterator();
		byte[] right = havingCols.getRight().getBytes(StandardCharsets.UTF_8);
		int index = havingCols.getColMeta().getColIndex();
		try {
			while (it.next()){
                UnsafeRow row = getAllBinaryRow(it.getValue());
                switch (havingCols.getOperator()) {
                case "=":
                    if (!eq(row.getBinary(index),right)) {
                        sorter.insertRow(row);
                    }
                    break;
                case ">":
                    if (!gt(row.getBinary(index),right)) {
                        sorter.insertRow(row);
                    }
                    break;
                case "<":
                    if (!lt(row.getBinary(index),right)) {
                        sorter.insertRow(row);
                    }
                    break;
                case ">=":
                    if (!gt(row.getBinary(index),right) && eq(row.getBinary(index),right)) {
                        sorter.insertRow(row);
                    }
                    break;
                case "<=":
                    if (!lt(row.getBinary(index),right) && eq(row.getBinary(index),right)) {
                        sorter.insertRow(row);
                    }
                    break;
                case "!=":
                    if (!neq(row.getBinary(index),right)) {
                        sorter.insertRow(row);
                    }
                    break;
                }
            }
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

	private boolean lt(byte[] l, byte[] r) {
		return -1 != ByteUtil.compareNumberByte(l, r);
	}

	private boolean gt(byte[] l, byte[] r) {
		return 1 != ByteUtil.compareNumberByte(l, r);
	}

	private boolean eq(byte[] l, byte[] r) {
		return 0 != ByteUtil.compareNumberByte(l, r);
	}

	private boolean neq(byte[] l, byte[] r) {
		return 0 == ByteUtil.compareNumberByte(l, r);
	}

	/**
	 * 构造groupKey
	 */
	private UnsafeRow getGroupKey(UnsafeRow row) throws UnsupportedEncodingException {

		UnsafeRow key = null;
		if(this.sortColumnsByIndex == null){
            /**
             * 针对没有group by关键字
             * select count(*) from table;
             */
			key = new UnsafeRow(this.groupKeyfieldCount+1);
			bufferHolder = new BufferHolder(key,0);
			unsafeRowWriter = new UnsafeRowWriter(bufferHolder,this.groupKeyfieldCount+1);
			bufferHolder.reset();
			unsafeRowWriter.write(0,"same".getBytes());
			key.setTotalSize(bufferHolder.totalSize());
			return key;
		}


		key = new UnsafeRow(this.groupKeyfieldCount);
		bufferHolder = new BufferHolder(key,0);
		unsafeRowWriter = new UnsafeRowWriter(bufferHolder,this.groupKeyfieldCount);
		bufferHolder.reset();


		ColMeta curColMeta = null;
		for (int i = 0; i < this.groupKeyfieldCount;i++) {
			curColMeta = this.columToIndx.get(sortColumnsByIndex[i].toUpperCase());
		     if(!row.isNullAt(curColMeta.colIndex)){
				switch(curColMeta.colType){
					case ColMeta.COL_TYPE_BIT:
						key.setByte(i,row.getByte(curColMeta.colIndex));
					case ColMeta.COL_TYPE_INT:
					case ColMeta.COL_TYPE_LONG:
					case ColMeta.COL_TYPE_INT24:
						key.setInt(i,
								BytesTools.getInt(row.getBinary(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_SHORT:
						key.setShort(i,
								BytesTools.getShort(row.getBinary(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_FLOAT:
						key.setFloat(i,
								 BytesTools.getFloat(row.getBinary(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_DOUBLE:
					case ColMeta.COL_TYPE_NEWDECIMAL:
						key.setDouble(i,
								BytesTools.getDouble(row.getBinary(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_LONGLONG:
						key.setLong(i,
								BytesTools.getLong(row.getBinary(curColMeta.colIndex)));
						break;
					default:
						unsafeRowWriter.write(i,
								row.getBinary(curColMeta.colIndex));
						break;
				}
		     }else {
				 key.setNullAt(i);
			 }
		}

		key.setTotalSize(bufferHolder.totalSize());

		return key;
	}


	/**
	 * 构造value
	 */
	private UnsafeRow getValue(UnsafeRow row) throws UnsupportedEncodingException {

		UnsafeRow value = new UnsafeRow(this.valuefieldCount);
		bufferHolder = new BufferHolder(value,0);
		unsafeRowWriter = new UnsafeRowWriter(bufferHolder,this.valuefieldCount);
		bufferHolder.reset();
		ColMeta curColMeta = null;
		for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
			curColMeta = fieldEntry.getValue();
			if(!row.isNullAt(curColMeta.colIndex)) {
				switch (curColMeta.colType) {
					case ColMeta.COL_TYPE_BIT:
						value.setByte(curColMeta.colIndex, row.getByte(curColMeta.colIndex));
						break;
					case ColMeta.COL_TYPE_INT:
					case ColMeta.COL_TYPE_LONG:
					case ColMeta.COL_TYPE_INT24:
						value.setInt(curColMeta.colIndex,
								BytesTools.getInt(row.getBinary(curColMeta.colIndex)));

						break;
					case ColMeta.COL_TYPE_SHORT:
						value.setShort(curColMeta.colIndex,
								BytesTools.getShort(row.getBinary(curColMeta.colIndex)));
						break;
					case ColMeta.COL_TYPE_LONGLONG:
						value.setLong(curColMeta.colIndex,
								BytesTools.getLong(row.getBinary(curColMeta.colIndex)));


						break;
					case ColMeta.COL_TYPE_FLOAT:
						value.setFloat(curColMeta.colIndex,
								BytesTools.getFloat(row.getBinary(curColMeta.colIndex)));

						break;
					case ColMeta.COL_TYPE_DOUBLE:
					case ColMeta.COL_TYPE_NEWDECIMAL:
						value.setDouble(curColMeta.colIndex, BytesTools.getDouble(row.getBinary(curColMeta.colIndex)));

						break;
					default:
						unsafeRowWriter.write(curColMeta.colIndex,
								row.getBinary(curColMeta.colIndex));
						break;
				}
			}else {
				value.setNullAt(curColMeta.colIndex);
			}
		}


		value.setTotalSize(bufferHolder.totalSize());
		return value;
	}

	public void addRow(UnsafeRow rowDataPkg) throws UnsupportedEncodingException {
		UnsafeRow key = getGroupKey(rowDataPkg);
		UnsafeRow value = getValue(rowDataPkg);

		if(aggregationMap.find(key)){
			UnsafeRow rs = aggregationMap.getAggregationBuffer(key);
			aggregateRow(rs,value);
		}else {
			aggregationMap.put(key,value);
		}

		return;
	}


	private boolean isMergeAvg(){

		if (mergCols == null) {
			return false;
		}

		for (MergeCol merg : mergCols) {
			if(merg.mergeType == MergeCol.MERGE_AVG) {
				return true;
			}
		}
		return false;
	}

	private void aggregateRow(UnsafeRow toRow, UnsafeRow newRow) throws UnsupportedEncodingException {
		if (mergCols == null) {
			return;
		}

		for (MergeCol merg : mergCols) {
             if(merg.mergeType != MergeCol.MERGE_AVG) {
				 byte[] result = null;
				 byte[] left = null;
				 byte[] right = null;
				 int type = merg.colMeta.colType;
				 int index = merg.colMeta.colIndex;
				 switch(type){
					 case ColMeta.COL_TYPE_INT:
					 case ColMeta.COL_TYPE_LONG:
					 case ColMeta.COL_TYPE_INT24:
						 left = BytesTools.int2Bytes(toRow.getInt(index));
						 right = BytesTools.int2Bytes(newRow.getInt(index));
						 break;
					 case ColMeta.COL_TYPE_SHORT:
						 left = BytesTools.short2Bytes(toRow.getShort(index));
						 right =BytesTools.short2Bytes(newRow.getShort(index));
						 break;
					 case ColMeta.COL_TYPE_LONGLONG:
						 left = BytesTools.long2Bytes(toRow.getLong(index));
						 right = BytesTools.long2Bytes(newRow.getLong(index));
						 break;
					 case ColMeta.COL_TYPE_FLOAT:
						 left = BytesTools.float2Bytes(toRow.getFloat(index));
						 right = BytesTools.float2Bytes(newRow.getFloat(index));
						 break;
					 case ColMeta.COL_TYPE_DOUBLE:
					 case ColMeta.COL_TYPE_NEWDECIMAL:
						 left = BytesTools.double2Bytes(toRow.getDouble(index));
						 right = BytesTools.double2Bytes(newRow.getDouble(index));
						 break;
					 default:
						 break;
				 }

                 result = mertFields(left,right,type,merg.mergeType);

				 if (result != null) {
					 switch(type){
					 	 case ColMeta.COL_TYPE_BIT:
					 	 	toRow.setByte(index,result[0]);
						 case ColMeta.COL_TYPE_INT:
						 case ColMeta.COL_TYPE_LONG:
						 case ColMeta.COL_TYPE_INT24:
							toRow.setInt(index,BytesTools.getInt(result));
							 break;
						 case ColMeta.COL_TYPE_SHORT:
							 toRow.setShort(index,BytesTools.getShort(result));
							 break;
						 case ColMeta.COL_TYPE_LONGLONG:
							 toRow.setLong(index,BytesTools.getLong(result));
							 break;
						 case ColMeta.COL_TYPE_FLOAT:
							 toRow.setFloat(index,BytesTools.getFloat(result));
							 break;
						 case ColMeta.COL_TYPE_DOUBLE:
						 case ColMeta.COL_TYPE_NEWDECIMAL:
                             toRow.setDouble(index,BytesTools.getDouble(result));
							 break;
						 default:
							 break;
					 }
                 }
             }
		}
    }

	private void mergAvg(UnsafeRow toRow) throws UnsupportedEncodingException {

		if (mergCols == null) {
			return;
		}

		for (MergeCol merg : mergCols) {
			if(merg.mergeType==MergeCol.MERGE_AVG) {
				byte[] result = null;
				byte[] avgSum = null;
				byte[] avgCount = null;

				int type = merg.colMeta.colType;
				int avgSumIndex = merg.colMeta.avgSumIndex;
				int avgCountIndex = merg.colMeta.avgCountIndex;

				switch(type){
					case ColMeta.COL_TYPE_BIT:
						avgSum = BytesTools.toBytes(toRow.getByte(avgSumIndex));
						avgCount = BytesTools.toBytes(toRow.getLong(avgCountIndex));
						break;
					case ColMeta.COL_TYPE_INT:
					case ColMeta.COL_TYPE_LONG:
					case ColMeta.COL_TYPE_INT24:
						avgSum = BytesTools.int2Bytes(toRow.getInt(avgSumIndex));
						avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
						break;
					case ColMeta.COL_TYPE_SHORT:
						avgSum =BytesTools.short2Bytes(toRow.getShort(avgSumIndex));
						avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
						break;

					case ColMeta.COL_TYPE_LONGLONG:
						avgSum = BytesTools.long2Bytes(toRow.getLong(avgSumIndex));
						avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));

						break;
					case ColMeta.COL_TYPE_FLOAT:
						avgSum = BytesTools.float2Bytes(toRow.getFloat(avgSumIndex));
						avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));

						break;
					case ColMeta.COL_TYPE_DOUBLE:
					case ColMeta.COL_TYPE_NEWDECIMAL:
						avgSum = BytesTools.double2Bytes(toRow.getDouble(avgSumIndex));
						avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
						break;
					default:
						break;
				}

				result = mertFields(avgSum,avgCount,merg.colMeta.colType,merg.mergeType);

				if (result != null) {
                    switch(type){
                    	case ColMeta.COL_TYPE_BIT:
                    		toRow.setByte(avgSumIndex,result[0]);
							break;
                        case ColMeta.COL_TYPE_INT:
						case ColMeta.COL_TYPE_LONG:
						case ColMeta.COL_TYPE_INT24:
                            toRow.setInt(avgSumIndex,BytesTools.getInt(result));
                            break;
                        case ColMeta.COL_TYPE_SHORT:
                            toRow.setShort(avgSumIndex,BytesTools.getShort(result));
                            break;
                        case ColMeta.COL_TYPE_LONGLONG:
                            toRow.setLong(avgSumIndex,BytesTools.getLong(result));
                            break;
                        case ColMeta.COL_TYPE_FLOAT:
                            toRow.setFloat(avgSumIndex,BytesTools.getFloat(result));
                            break;
                        case ColMeta.COL_TYPE_DOUBLE:
                        case ColMeta.COL_TYPE_NEWDECIMAL:
                            toRow.setDouble(avgSumIndex,ByteUtil.getDouble(result));
                            break;
                        default:
                            break;
                    }
				}
			}
		}
	}

	private byte[] mertFields(byte[] bs, byte[] bs2, int colType, int mergeType) throws UnsupportedEncodingException {

		if(bs2==null || bs2.length==0) {
			return bs;
		}else if(bs==null || bs.length==0) {
			return bs2;
		}

		switch (mergeType) {
			case MergeCol.MERGE_SUM:
				if (colType == ColMeta.COL_TYPE_NEWDECIMAL
						|| colType == ColMeta.COL_TYPE_DECIMAL
						|| colType == ColMeta.COL_TYPE_DOUBLE
						|| colType == ColMeta.COL_TYPE_FLOAT){
					double vale = BytesTools.getDouble(bs) +
							BytesTools.getDouble(bs2);
					return BytesTools.double2Bytes(vale);
				}

			case MergeCol.MERGE_COUNT: {
				long s1 = BytesTools.getLong(bs);
				long s2 = BytesTools.getLong(bs2);
				long total = s1 + s2;
				return BytesTools.long2Bytes(total);
			}

			case MergeCol.MERGE_MAX: {
				int compare = BytesTools.compareTo(bs, bs2);
				return (compare > 0) ? bs : bs2;
			}

			case MergeCol.MERGE_MIN: {
				int compare = BytesTools.compareTo(bs, bs2);
				return (compare > 0) ? bs2 : bs;

			}
			case MergeCol.MERGE_AVG: {
				/**
				 * 数值总和
				 */
				double sum = BytesTools.getDouble(bs);

				/**
				 * 元素总个数
				 */
				long count = BytesTools.getLong(bs2);
				double value = sum / count;
				NumberFormat nf = NumberFormat.getNumberInstance();
				nf.setMaximumFractionDigits(4);
				return BytesTools.double2Bytes(value);
			}
			default:
				return null;
		}
	}

	public void free(){
		if(aggregationMap != null)
		aggregationMap.free();
	}
}
