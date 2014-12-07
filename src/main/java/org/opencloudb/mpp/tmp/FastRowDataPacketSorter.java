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
package org.opencloudb.mpp.tmp;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

import org.opencloudb.mpp.ColMeta;
import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.RowDataPacketSorter;
import org.opencloudb.util.ByteUtil;
import org.opencloudb.util.CompareUtil;
import org.opencloudb.net.mysql.RowDataPacket;

/***
 * RowDataPacketSorter
 * 
 * @author czp:2014年12月7日
 *
 */
public class FastRowDataPacketSorter extends RowDataPacketSorter {

	protected OrderCol[] orderCols;
	private List<RowDataPacket> sorted;

	public FastRowDataPacketSorter(OrderCol[] orderCols) {
		super(orderCols);
		this.orderCols = orderCols;
		this.sorted = new ArrayList<RowDataPacket>();
	}

	public void addRow(RowDataPacket row) {
		this.sorted.add(row);

	}

	public Collection<RowDataPacket> getSortedResult() {
		RowDataPacket[] arr = new RowDataPacket[sorted.size()];
		sorted.toArray(arr);
		sorted.clear();// for GC
		try {
			TimMergeSort.sort(arr, 0, arr.length, new Comparator<RowDataPacket>() {

				@Override
				public int compare(RowDataPacket o1, RowDataPacket o2) {
					OrderCol[] tmp = orderCols;
					int cmp = 0;
					int len = tmp.length;
					int type = OrderCol.COL_ORDER_TYPE_ASC;
					for (int i = 0; i < len; i++) {
						int colIndex = tmp[i].colMeta.colIndex;
						byte[] left = o1.fieldValues.get(colIndex);
						byte[] right = o2.fieldValues.get(colIndex);
						if (tmp[i].orderType == type) {
							cmp = compareObject(left, right, tmp[colIndex]);
						} else {
							cmp = compareObject(right, left, tmp[colIndex]);
						}
						if (cmp != 0)
							return cmp;
					}
					return 0;
				}

			}, null, 0, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (RowDataPacket row : arr) {
			sorted.add(row);
		}
		return sorted;
	}

	protected int compareObject(byte[] left, byte[] right, OrderCol orderCol) {

		switch (orderCol.getColMeta().getColType()) {
		case ColMeta.COL_TYPE_INT:
		case ColMeta.COL_TYPE_LONG:
		case ColMeta.COL_TYPE_SHORT:
		case ColMeta.COL_TYPE_FLOAT:
		case ColMeta.COL_TYPE_INT24:
		case ColMeta.COL_TYPE_DOUBLE:
		case ColMeta.COL_TYPE_DECIMAL:
		case ColMeta.COL_TYPE_LONGLONG:
		case ColMeta.COL_TYPE_NEWDECIMAL:
		case ColMeta.COL_TYPE_BIT:
		case ColMeta.COL_TYPE_DATE:
		case ColMeta.COL_TYPE_TIME:
		case ColMeta.COL_TYPE_YEAR:
		case ColMeta.COL_TYPE_NEWDATE:
		case ColMeta.COL_TYPE_DATETIME:
		case ColMeta.COL_TYPE_TIMSTAMP:
			// 因为mysql的日期也是数字字符串方式表达，因此可以跟整数等一起对待
			return ByteUtil.compareNumberByte(left, right);
		case ColMeta.COL_TYPE_STRING:
		case ColMeta.COL_TYPE_VAR_STRING:
			// ENUM和SET类型都是字符串，按字符串处理
		case ColMeta.COL_TYPE_SET:
		case ColMeta.COL_TYPE_ENUM:
			return CompareUtil.compareString(ByteUtil.getString(left),
					ByteUtil.getString(right));
			// BLOB相关类型和GEOMETRY类型不支持排序，略掉
		}
		return 0;
	}
}