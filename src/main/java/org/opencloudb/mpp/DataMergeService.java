package org.opencloudb.mpp;

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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.mpp.tmp.RowDataSorter;
import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.nio.handler.MultiNodeQueryHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.StringUtil;

/**
 * Data merge service handle data Min,Max,AVG group 、order by 、limit
 * 
 * @author wuzhih /modify by coder_czp/2015/11/2
 * 
 */
public class DataMergeService implements Runnable {

	// 保存包和节点的关系
	class PackWraper {
		public byte[] data;
		public String node;

	}

	private int fieldCount;
	private RouteResultset rrs;
	private RowDataSorter sorter;
	private RowDataPacketGrouper grouper;
	private volatile boolean hasOrderBy = false;
	private MultiNodeQueryHandler multiQueryHandler;
	public PackWraper END_FLAG_PACK = new PackWraper();
	private AtomicInteger areadyAdd = new AtomicInteger();
	private List<RowDataPacket> result = new Vector<RowDataPacket>();
	private static Logger LOGGER = Logger.getLogger(DataMergeService.class);
	private BlockingQueue<PackWraper> packs = new LinkedBlockingQueue<PackWraper>();
	private ConcurrentHashMap<String, Boolean> canDiscard = new ConcurrentHashMap<String, Boolean>();

	public DataMergeService(MultiNodeQueryHandler handler, RouteResultset rrs) {
		this.rrs = rrs;
		this.multiQueryHandler = handler;
	}

	public RouteResultset getRrs() {
		return this.rrs;
	}

	public void outputMergeResult(NonBlockingSession session, byte[] eof) {
		packs.add(END_FLAG_PACK);
	}

	public void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("field metadata inf:" + columToIndx.entrySet());
		}
		int[] groupColumnIndexs = null;
		this.fieldCount = fieldCount;
		if (rrs.getGroupByCols() != null) {
			groupColumnIndexs = toColumnIndex(rrs.getGroupByCols(), columToIndx);
		}

		if (rrs.getHavingCols() != null) {
			ColMeta colMeta = columToIndx.get(rrs.getHavingCols().getLeft()
					.toUpperCase());
			if (colMeta != null) {
				rrs.getHavingCols().setColMeta(colMeta);
			}
		}

		if (rrs.isHasAggrColumn()) {
			List<MergeCol> mergCols = new LinkedList<MergeCol>();
			Map<String, Integer> mergeColsMap = rrs.getMergeCols();
			if (mergeColsMap != null) {
				for (Map.Entry<String, Integer> mergEntry : mergeColsMap
						.entrySet()) {
					String colName = mergEntry.getKey().toUpperCase();
					int type = mergEntry.getValue();
					if (MergeCol.MERGE_AVG == type) {
						ColMeta sumColMeta = columToIndx.get(colName + "SUM");
						ColMeta countColMeta = columToIndx.get(colName
								+ "COUNT");
						if (sumColMeta != null && countColMeta != null) {
							ColMeta colMeta = new ColMeta(sumColMeta.colIndex,
									countColMeta.colIndex,
									sumColMeta.getColType());
							mergCols.add(new MergeCol(colMeta, mergEntry
									.getValue()));
						}
					} else {
						ColMeta colMeta = columToIndx.get(colName);
						mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
					}
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
					mergCols.toArray(new MergeCol[mergCols.size()]),
					rrs.getHavingCols());
		}
		if (rrs.getOrderByCols() != null) {
			LinkedHashMap<String, Integer> orders = rrs.getOrderByCols();
			OrderCol[] orderCols = new OrderCol[orders.size()];
			int i = 0;
			for (Map.Entry<String, Integer> entry : orders.entrySet()) {
				String key = StringUtil.removeBackquote(entry.getKey()
						.toUpperCase());
				ColMeta colMeta = columToIndx.get(key);
				if (colMeta == null) {
					throw new java.lang.IllegalArgumentException(
							"all columns in order by clause should be in the selected column list!"
									+ entry.getKey());
				}
				orderCols[i++] = new OrderCol(colMeta, entry.getValue());
			}
			// sorter = new RowDataPacketSorter(orderCols);
			RowDataSorter tmp = new RowDataSorter(orderCols);
			tmp.setLimit(rrs.getLimitStart(), rrs.getLimitSize());
			hasOrderBy = true;
			sorter = tmp;
		} else {
			hasOrderBy = false;
		}
		MycatServer.getInstance().getBusinessExecutor().execute(this);
	}

	/**
	 * process new record (mysql binary data),if data can output to client
	 * ,return true
	 * 
	 * @param dataNode
	 *            DN's name (data from this dataNode)
	 * @param rowData
	 *            raw data
	 * @param conn
	 */
	public boolean onNewRecord(String dataNode, byte[] rowData) {
		// 对于无需排序的SQL,取前getLimitSize条就足够
        //可能有聚合函数等场景会误判，所有先注释
//		if (!hasOrderBy && areadyAdd.get() >= rrs.getLimitSize()&& rrs.getLimitSize()!=-1) {
//			return true;
//		}
		// 对于需要排序的数据,由于mysql传递过来的数据是有序的,
		// 如果某个节点的当前数据已经不会进入,后续的数据也不会入堆
		if (canDiscard.size() == rrs.getNodes().length) {
			LOGGER.error("now we output to client");
			packs.add(END_FLAG_PACK);
			return true;
		}
		if (canDiscard.get(dataNode) != null) {
			return true;
		}
		PackWraper data = new PackWraper();
		data.node = dataNode;
		data.data = rowData;
		packs.add(data);
		areadyAdd.getAndIncrement();
		return false;
	}

	private static int[] toColumnIndex(String[] columns,
			Map<String, ColMeta> toIndexMap) {
		int[] result = new int[columns.length];
		ColMeta curColMeta;
		for (int i = 0; i < columns.length; i++) {
			curColMeta = toIndexMap.get(columns[i].toUpperCase());
			if (curColMeta == null) {
				throw new java.lang.IllegalArgumentException(
						"all columns in group by clause should be in the selected column list.!"
								+ columns[i]);
			}
			result[i] = curColMeta.colIndex;
		}
		return result;
	}

	/**
	 * release resources
	 */
	public void clear() {
		result.clear();
		hasOrderBy = false;
		grouper = null;
		sorter = null;
	}

	@Override
	public void run() {

		int warningCount = 0;
		EOFPacket eofp = new EOFPacket();
		ByteBuffer eof = ByteBuffer.allocate(9);
		BufferUtil.writeUB3(eof, eofp.calcPacketSize());
		eof.put(eofp.packetId);
		eof.put(eofp.fieldCount);
		BufferUtil.writeUB2(eof, warningCount);
		BufferUtil.writeUB2(eof, eofp.status);
		ServerConnection source = multiQueryHandler.getSession().getSource();

		while (!Thread.interrupted()) {
			try {
				PackWraper pack = packs.take();
				if (pack == END_FLAG_PACK) {
					break;
				}
				RowDataPacket row = new RowDataPacket(fieldCount);
				row.read(pack.data);
				if (grouper != null) {
					grouper.addRow(row);
				} else if (sorter != null) {
					if (!sorter.addRow(row)) {
						canDiscard.put(pack.node, true);
					}
				} else {
					result.add(row);
				}
			} catch (Exception e) {
				LOGGER.error("Merge multi data error", e);
			}
		}
		byte[] array = eof.array();
		multiQueryHandler.outputMergeResult(source, array, getResults(array));
	}

	/**
	 * return merged data
	 * 
	 * @return (最多i*(offset+size)行数据)
	 */
	private List<RowDataPacket> getResults(byte[] eof) {
		List<RowDataPacket> tmpResult = result;
		if (this.grouper != null) {
			tmpResult = grouper.getResult();
			grouper = null;
		}
		if (sorter != null) {
			// 处理grouper处理后的数据
			if (tmpResult != null) {
				Iterator<RowDataPacket> itor = tmpResult.iterator();
				while (itor.hasNext()) {
					sorter.addRow(itor.next());
					itor.remove();
				}
			}
			tmpResult = sorter.getSortedResult();
			sorter = null;
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("prepare mpp merge result for " + rrs.getStatement());
		}

		return tmpResult;
	}
}
