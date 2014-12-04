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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;

/**
 * Data merge service handle data Min,Max,AVG group 、order by 、limit
 * 
 * @author wuzhih
 * 
 */
public class DataMergeService {
	private static final Logger LOGGER = Logger
			.getLogger(DataMergeService.class);
	private RowDataPacketGrouper grouper = null;
	private RowDataPacketSorter sorter = null;
	private Collection<RowDataPacket> result = new LinkedList<RowDataPacket>();

	// private final Map<String, DataNodeResultInf> dataNodeResultSumMap;

	public DataMergeService(RouteResultset rrs) {
		this.rrs = rrs;
		// dataNodeResultSumMap = new HashMap<String, DataNodeResultInf>(
		// rrs.getNodes().length);

	}

	/**
	 * return merged data
	 * 
	 * @return
	 */
	public Collection<RowDataPacket> getResults() {
		Collection<RowDataPacket> tmpResult = result;
		if (this.grouper != null) {
			tmpResult = grouper.getResult();
			grouper = null;
		}
		if (sorter != null) {
			Iterator<RowDataPacket> itor = tmpResult.iterator();
			while (itor.hasNext()) {
				sorter.addRow(itor.next());
				itor.remove();

			}
			tmpResult = sorter.getSortedResult();
			sorter = null;
		}
		return tmpResult;
	}

	public void setFieldCount(int fieldCount) {
		this.fieldCount = fieldCount;
	}

	public RouteResultset getRrs() {
		return rrs;
	}

	private int fieldCount;
	private final RouteResultset rrs;

	public void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("field metadata inf:"
					+ Arrays.toString(columToIndx.entrySet().toArray()));
		}
		int[] groupColumnIndexs = null;
		this.fieldCount = fieldCount;
		if (rrs.getGroupByCols() != null) {
			groupColumnIndexs = (toColumnIndex(rrs.getGroupByCols(),
					columToIndx));
		}
		if (rrs.isHasAggrColumn()) {
			List<MergeCol> mergCols = new LinkedList<MergeCol>();
			if (rrs.getMergeCols() != null) {
				for (Map.Entry<String, Integer> mergEntry : rrs.getMergeCols()
						.entrySet()) {
					String colName = mergEntry.getKey().toUpperCase();
					ColMeta colMeta = columToIndx.get(colName);
					mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
				}
			}
			// add no alias merg column
			for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
				String colName = fieldEntry.getKey();
				int result = MergeCol.tryParseAggCol(colName);
				if (result != MergeCol.MERGE_UNSUPPORT
						&& result != MergeCol.MERGE_NOMERGE) {
					mergCols.add(new MergeCol(fieldEntry.getValue(), result));
				}
			}
			grouper = new RowDataPacketGrouper(groupColumnIndexs,
					mergCols.toArray(new MergeCol[mergCols.size()]));
		}
		if (rrs.getOrderByCols() != null) {
			LinkedHashMap<String, Integer> orders = rrs.getOrderByCols();
			OrderCol[] orderCols = new OrderCol[orders.size()];
			int i = 0;
			for (Map.Entry<String, Integer> entry : orders.entrySet()) {
				orderCols[i++] = new OrderCol(columToIndx.get(entry.getKey()
						.toUpperCase()), entry.getValue());
			}
			sorter = new RowDataPacketSorter(orderCols);
		} else {
			result = new LinkedList<RowDataPacket>();
		}
	}

	/**
	 * process new record (mysql binary data),if data can output to client
	 * ,return true
	 * 
	 * @param dataNode
	 *            DN's name (data from this dataNode)
	 * @param rowData
	 *            raw data
	 */
	public boolean onNewRecord(String dataNode, byte[] rowData) {
		RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
		rowDataPkg.read(rowData);
		if (grouper != null) {
			grouper.addRow(rowDataPkg);
		} else {
			result.add(rowDataPkg);
		}
		// todo ,if too large result set ,should store to disk
		return false;

	}

	private static int[] toColumnIndex(String[] columns,
			Map<String, ColMeta> toIndexMap) {
		int[] result = new int[columns.length];
		ColMeta curColMeta = null;
		for (int i = 0; i < columns.length; i++) {
			curColMeta = toIndexMap.get(columns[i].toUpperCase());
			if (curColMeta == null) {
				throw new java.lang.IllegalArgumentException(
						"can't find column in select fields " + columns[i]);
			}
			result[i] = curColMeta.colIndex;
		}
		return result;
	}

	/**
	 * release resources
	 */
	public void clear() {
		grouper = null;
		sorter = null;
		result = null;
	}

}

class DataNodeResultInf {
	public RowDataPacket firstRecord;
	public RowDataPacket lastRecord;
	public int rowCount;

}