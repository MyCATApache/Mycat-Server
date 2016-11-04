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
package org.opencloudb.mpp;

import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.ByteUtil;
import org.opencloudb.util.LongUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * implement group function select a,count(*),sum(*) from A group by a
 * 
 * @author wuzhih
 * 
 */
public class RowDataPacketGrouper {

	private List<RowDataPacket> result = Collections.synchronizedList(new ArrayList<RowDataPacket>());
	private final MergeCol[] mergCols;
	private final int[] groupColumnIndexs;
	private boolean isMergAvg=false;
	private HavingCols havingCols;

	public RowDataPacketGrouper(int[] groupColumnIndexs, MergeCol[] mergCols,HavingCols havingCols) {
		super();
		this.groupColumnIndexs = groupColumnIndexs;
		this.mergCols = mergCols;
		this.havingCols = havingCols;
	}

	public List<RowDataPacket> getResult() {
		if(!isMergAvg)
		{
			for (RowDataPacket row : result)
			{
				mergAvg(row);
			}
			isMergAvg=true;
		}

		if(havingCols != null){
			filterHaving();
		}

		return result;
	}

	private void filterHaving(){
		if (havingCols.getColMeta() == null || result == null) {
			return;
		}
		Iterator<RowDataPacket> it = result.iterator();
		byte[] right = havingCols.getRight().getBytes(
				StandardCharsets.UTF_8);
		int index = havingCols.getColMeta().getColIndex();
		while (it.hasNext()){
			RowDataPacket rowDataPacket = it.next();
			switch (havingCols.getOperator()) {
			case "=":
				if (eq(rowDataPacket.fieldValues.get(index),right)) {
					it.remove();
				}
				break;
			case ">":
				if (gt(rowDataPacket.fieldValues.get(index),right)) {
					it.remove();
				}
				break;
			case "<":
				if (lt(rowDataPacket.fieldValues.get(index),right)) {
					it.remove();
				}
				break;
			case ">=":
				if (gt(rowDataPacket.fieldValues.get(index),right) && eq(rowDataPacket.fieldValues.get(index),right)) {
					it.remove();
				}
				break;
			case "<=":
				if (lt(rowDataPacket.fieldValues.get(index),right) && eq(rowDataPacket.fieldValues.get(index),right)) {
					it.remove();
				}
				break;
			case "!=":
				if (neq(rowDataPacket.fieldValues.get(index),right)) {
					it.remove();
				}
				break;
			}
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

	public void addRow(RowDataPacket rowDataPkg) {
		for (RowDataPacket row : result) {
			if (sameGropuColums(rowDataPkg, row)) {
				aggregateRow(row, rowDataPkg);
				return;
			}
		}

		// not aggreated ,insert new
		result.add(rowDataPkg);

	}

	private void aggregateRow(RowDataPacket toRow, RowDataPacket newRow) {
		if (mergCols == null) {
			return;
		}
		for (MergeCol merg : mergCols) {
             if(merg.mergeType!=MergeCol.MERGE_AVG)
             {
                 byte[] result = mertFields(
                         toRow.fieldValues.get(merg.colMeta.colIndex),
                         newRow.fieldValues.get(merg.colMeta.colIndex),
                         merg.colMeta.colType, merg.mergeType);
                 if (result != null)
                 {
                     toRow.fieldValues.set(merg.colMeta.colIndex, result);
                 }
             }
		}




    }

	private void mergAvg(RowDataPacket toRow) {
		if (mergCols == null) {
			return;
		}

		Set<Integer> rmIndexSet = new HashSet<Integer>();
		for (MergeCol merg : mergCols) {
			if(merg.mergeType==MergeCol.MERGE_AVG)
			{
				byte[] result = mertFields(
						toRow.fieldValues.get(merg.colMeta.avgSumIndex),
						toRow.fieldValues.get(merg.colMeta.avgCountIndex),
						merg.colMeta.colType, merg.mergeType);
				if (result != null)
				{
					toRow.fieldValues.set(merg.colMeta.avgSumIndex, result);
//					toRow.fieldValues.remove(merg.colMeta.avgCountIndex) ;
//					toRow.fieldCount=toRow.fieldCount-1;
					rmIndexSet.add(merg.colMeta.avgCountIndex);
				}
			}
		}
		for(Integer index : rmIndexSet) {
			toRow.fieldValues.remove(index);
			toRow.fieldCount = toRow.fieldCount - 1;
		}


	}

	private byte[] mertFields(byte[] bs, byte[] bs2, int colType, int mergeType) {
		// System.out.println("mergeType:"+ mergeType+" colType "+colType+
		// " field:"+Arrays.toString(bs)+ " ->  "+Arrays.toString(bs2));
		if(bs2==null || bs2.length==0)
		{
			return bs;
		}else if(bs==null || bs.length==0)
		{
			return bs2;
		}
		switch (mergeType) {
		case MergeCol.MERGE_SUM:
			if (colType == ColMeta.COL_TYPE_DOUBLE
				|| colType == ColMeta.COL_TYPE_FLOAT) {

				Double vale = ByteUtil.getDouble(bs) + ByteUtil.getDouble(bs2);
				return vale.toString().getBytes();
				// return String.valueOf(vale).getBytes();
			} else if(colType == ColMeta.COL_TYPE_NEWDECIMAL
					|| colType == ColMeta.COL_TYPE_DECIMAL) {
				BigDecimal d1 = new BigDecimal(new String(bs));
				d1 = d1.add(new BigDecimal(new String(bs2)));
				return String.valueOf(d1).getBytes();
			}
			// continue to count case
		case MergeCol.MERGE_COUNT: {
			long s1 = Long.parseLong(new String(bs));
			long s2 = Long.parseLong(new String(bs2));
			long total = s1 + s2;
			return LongUtil.toBytes(total);
		}
		case MergeCol.MERGE_MAX: {
			// System.out.println("value:"+
			// ByteUtil.getNumber(bs).doubleValue());
			// System.out.println("value2:"+
			// ByteUtil.getNumber(bs2).doubleValue());
			// int compare = CompareUtil.compareDouble(ByteUtil.getNumber(bs)
			// .doubleValue(), ByteUtil.getNumber(bs2).doubleValue());
			// return ByteUtil.compareNumberByte(bs, bs2);
			int compare = ByteUtil.compareNumberByte(bs, bs2);
			return (compare > 0) ? bs : bs2;

		}
		case MergeCol.MERGE_MIN: {
			// int compare = CompareUtil.compareDouble(ByteUtil.getNumber(bs)
			// .doubleValue(), ByteUtil.getNumber(bs2).doubleValue());
			// int compare = ByteUtil.compareNumberArray(bs, bs2);
			//return (compare > 0) ? bs2 : bs;
			int compare = ByteUtil.compareNumberByte(bs, bs2);
			return (compare > 0) ? bs2 : bs;
			// return ByteUtil.compareNumberArray2(bs, bs2, 2);
		}
            case MergeCol.MERGE_AVG: {
            	if (colType == ColMeta.COL_TYPE_DOUBLE
    					|| colType == ColMeta.COL_TYPE_FLOAT) {
            		double aDouble = ByteUtil.getDouble(bs);
            		long s2 = Long.parseLong(new String(bs2));
            		Double vale = aDouble / s2;
            		return vale.toString().getBytes();
            	} else if(colType == ColMeta.COL_TYPE_NEWDECIMAL
    					|| colType == ColMeta.COL_TYPE_DECIMAL) {
            		BigDecimal sum = new BigDecimal(new String(bs));
                    // mysql avg 处理精度为 sum结果的精度扩展4, 采用四舍五入
                    BigDecimal avg = sum.divide(new BigDecimal(new String(bs2)), sum.scale() + 4, RoundingMode.HALF_UP);
                    return avg.toString().getBytes();
            	}
            }
		default:
			return null;
		}

	}

	// private static final

	private boolean sameGropuColums(RowDataPacket newRow, RowDataPacket existRow) {
		if (groupColumnIndexs == null) {// select count(*) from aaa , or group
										// column
			return true;
		}
		for (int i = 0; i < groupColumnIndexs.length; i++) {
			if (!Arrays.equals(newRow.fieldValues.get(groupColumnIndexs[i]),
					existRow.fieldValues.get(groupColumnIndexs[i]))) {
				return false;
			}

		}
		return true;

	}
}
