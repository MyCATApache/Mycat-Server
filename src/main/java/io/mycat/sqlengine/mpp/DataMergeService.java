package io.mycat.sqlengine.mpp;

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

import io.mycat.MycatServer;
import io.mycat.route.RouteResultset;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.executors.MultiNodeQueryHandler;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.server.packet.util.BufferUtil;
import io.mycat.sqlengine.tmp.RowDataSorter;
import io.mycat.util.StringUtil;

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

/**
 * Data merge service handle data Min,Max,AVG group 、order by 、limit
 * 
 * @author wuzhih /modify by coder_czp/2015/11/2
 * 
 */
public class DataMergeService implements Runnable {

	// 保存包和节点的关系
	static class PackWraper {
		byte[] data;
		String node;

	}

	private int fieldCount;
	private RouteResultset rrs;
	private RowDataSorter sorter;
	private RowDataPacketGrouper grouper;
	private int MAX_MUTIL_COUNT = 20000000;
	private BlockingQueue<PackWraper> packs;
	private volatile boolean hasOrderBy = false;
	private MultiNodeQueryHandler multiQueryHandler;
	private AtomicInteger areadyAdd = new AtomicInteger();
	private ConcurrentHashMap<String, Boolean> canDiscard;
	public static PackWraper END_FLAG_PACK = new PackWraper();
	private List<RowDataPacket> result = new Vector<RowDataPacket>();
	private static Logger LOGGER = Logger.getLogger(DataMergeService.class);

	public DataMergeService(MultiNodeQueryHandler handler, RouteResultset rrs) {
		this.rrs = rrs;
		this.multiQueryHandler = handler;
		this.canDiscard = new ConcurrentHashMap<String, Boolean>();
		// 在网络很好的情况下,数据会快速填充到队列,当数据大于MAX_MUTIL_COUNT时,等待处理线程处理
		this.packs = new LinkedBlockingQueue<PackWraper>(MAX_MUTIL_COUNT);
	}

	public RouteResultset getRrs() {
		return this.rrs;
	}

	public void outputMergeResult(NonBlockingSession session, byte[] eof) {
		packs.add(END_FLAG_PACK);
	}

	/**
	 * return merged data
	 * 
	 * @return (最多i*(offset+size)行数据)
	 */
	public List<RowDataPacket> getResults(byte[] eof) {
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
		MycatServer.getInstance().getListeningExecutorService().execute(this);
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
		try {
			// 对于无需排序的SQL,取前getLimitSize条就足够
            //由于聚合函数等场景可能有误判的情况，暂时先注释
//			if (!hasOrderBy && areadyAdd.get() >= rrs.getLimitSize()&& rrs.getLimitSize()!=-1) {
//				packs.add(END_FLAG_PACK);
//				return true;
//			}
			// 对于需要排序的数据,由于mysql传递过来的数据是有序的,
			// 如果某个节点的当前数据已经不会进入,后续的数据也不会入堆
			if (canDiscard.size() == rrs.getNodes().length) {
				packs.add(END_FLAG_PACK);
				LOGGER.info("other pack can discard,now send to client");
				return true;
			}
			if (canDiscard.get(dataNode) != null) {
				return true;
			}
			PackWraper data = new PackWraper();
			data.node = dataNode;
			data.data = rowData;
			packs.put(data);
			areadyAdd.getAndIncrement();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
		hasOrderBy = false;
		result.clear();
		grouper = null;
		sorter = null;
	}

	@Override
	public void run() {

		EOFPacket eofp = new EOFPacket();
		ByteBuffer eof = ByteBuffer.allocate(9);
		BufferUtil.writeUB3(eof, eofp.calcPacketSize());
		eof.put(eofp.packetId);
		eof.put(eofp.fieldCount);
		BufferUtil.writeUB2(eof, eofp.status);
		BufferUtil.writeUB2(eof, eofp.warningCount);
		MySQLFrontConnection source = multiQueryHandler.getSession()
				.getSource();

		while (!Thread.interrupted()) {
			try {
				PackWraper pack = packs.take();
				if (pack == END_FLAG_PACK) {
					multiQueryHandler.outputMergeResult(source, eof.array());
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
	}

}
