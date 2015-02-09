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

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.mpp.tmp.RowDataSorter;
import org.opencloudb.mysql.nio.handler.MultiNodeQueryHandler;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

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
	private Collection<RowDataPacket> result = new ConcurrentLinkedQueue<RowDataPacket>();
	private volatile boolean temniated = false;
	// private final Map<String, DataNodeResultInf> dataNodeResultSumMap;
	private final Map<String, AtomicReferenceArray<byte[]>> batchedNodeRows;
	private final ReentrantLock lock = new ReentrantLock();
	private final LinkedTransferQueue<Runnable> jobQueue = new LinkedTransferQueue<Runnable>();
	private volatile boolean jobRuninng = false;
	private final MultiNodeQueryHandler multiQueryHandler;
	private int fieldCount;
	private final RouteResultset rrs;

	public DataMergeService(MultiNodeQueryHandler queryHandler,
			RouteResultset rrs) {
		this.rrs = rrs;
		this.multiQueryHandler = queryHandler;
		batchedNodeRows = new HashMap<String, AtomicReferenceArray<byte[]>>();
		for (RouteResultsetNode node : rrs.getNodes()) {
			batchedNodeRows.put(node.getName(),
					new AtomicReferenceArray<byte[]>(100));
		}
	}

	public RouteResultset getRrs() {
		return this.rrs;
	}

	public void outputMergeResult(final NonBlockingSession session,
			final byte[] eof) {
		// handle remains batch data
		for (Entry<String, AtomicReferenceArray<byte[]>> entry : batchedNodeRows
				.entrySet()) {
			Runnable job = this.createJob(entry.getKey(), entry.getValue());
			if (job != null) {
				this.jobQueue.offer(job);
			}
		}
		// add output job
		Runnable outPutJob = new Runnable() {
			@Override
			public void run() {
				multiQueryHandler.outputMergeResult(session.getSource(), eof);
			}
		};
		jobQueue.offer(outPutJob);
		if (jobRuninng == false && !jobQueue.isEmpty()) {
			MycatServer.getInstance().getBusinessExecutor()
					.execute(jobQueue.poll());
		}
	}

	/**
	 * return merged data
	 * 
	 * @return (最多i*(offset+size)行数据)
	 */
	public Collection<RowDataPacket> getResults(final byte[] eof) {

		Collection<RowDataPacket> tmpResult = result;
		if (this.grouper != null) {
			tmpResult = grouper.getResult();
			grouper = null;
		}
		if (sorter != null) {
			//处理grouper处理后的数据
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
		if(LOGGER.isDebugEnabled())
		{
			LOGGER.debug("prepare mpp merge result for "+rrs.getStatement());
		}
		
		return tmpResult;
	}

	public void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("field metadata inf:"
					+ Arrays.toString(columToIndx.entrySet().toArray()));
		}
		int[] groupColumnIndexs = null;
		this.fieldCount = fieldCount;
		if (rrs.getGroupByCols() != null) {
			groupColumnIndexs = toColumnIndex(rrs.getGroupByCols(),
					columToIndx);
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
				ColMeta colMeta = columToIndx.get(entry.getKey().toUpperCase());
				if (colMeta == null) {
					throw new java.lang.IllegalArgumentException(
							"order by语句包含的字段必须出现在select语句字段列表里面" + entry.getKey().toUpperCase());
				}
				orderCols[i++] = new OrderCol(columToIndx.get(entry.getKey()
						.toUpperCase()), entry.getValue());
			}
//		 	sorter = new RowDataPacketSorter(orderCols);
			RowDataSorter tmp = new RowDataSorter(orderCols);
			tmp.setLimit(rrs.getLimitStart(), rrs.getLimitSize());
			sorter = tmp;
		} else {
			new ConcurrentLinkedQueue<RowDataPacket>();
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
		if (temniated) {
			return true;
		}
		Runnable batchJob = null;
		AtomicReferenceArray<byte[]> batchedArray = batchedNodeRows
				.get(dataNode);
		if (putBatchFailed(rowData, batchedArray)) {
			try {
				lock.lock();
				if (batchedArray.get(batchedArray.length() - 1) != null) {// full
					batchJob = createJob(dataNode, batchedArray);
				}
				putBatchFailed(rowData, batchedArray);

			} finally {
				lock.unlock();
			}
		}
		if (batchJob != null && jobRuninng == false) {
			MycatServer.getInstance().getBusinessExecutor().execute(batchJob);
		} else if (batchJob != null) {
			jobQueue.offer(batchJob);
		}

		return false;
	}

	private boolean handleRowData(String dataNode, byte[] rowData) {
		RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
		rowDataPkg.read(rowData);
		if (grouper != null) {
			grouper.addRow(rowDataPkg);
		} else if (sorter != null) {
			sorter.addRow(rowDataPkg);
		} else {
			result.add(rowDataPkg);
		}
		// todo ,if too large result set ,should store to disk
		return false;
	}

	private Runnable createJob(final String dnName,
			AtomicReferenceArray<byte[]> batchedArray) {
		final ArrayList<byte[]> rows = new ArrayList<byte[]>(
				batchedArray.length());
		for (int i = 0; i < batchedArray.length(); i++) {
			byte[] val = batchedArray.getAndSet(i, null);
			if (val != null) {
				rows.add(val);
			}
		}
		if (rows.isEmpty()) {
			return null;
		}
		Runnable job = new Runnable() {
			public void run() {
				try {
					jobRuninng = true;
					for (byte[] row : rows) {
						if (handleRowData(dnName, row)) {
							break;
						}
					}
					// for next job
					Runnable newJob = jobQueue.poll();
					if (newJob != null) {
						MycatServer.getInstance().getBusinessExecutor()
								.execute(newJob);
					} else {
						jobRuninng = false;
					}
				} catch (Exception e) {
					jobRuninng = false;
					LOGGER.warn("data Merge error:", e);
				}
			}
		};
		return job;
	}

	private boolean putBatchFailed(byte[] rowData,
			AtomicReferenceArray<byte[]> array) {
		int len = array.length();
		int i = 0;
		for (i = 0; i < len; i++) {
			if (array.compareAndSet(i, null, rowData))
				break;
		}
		return i == len;
	}

	private static int[] toColumnIndex(String[] columns,
			Map<String, ColMeta> toIndexMap) {
		int[] result = new int[columns.length];
		ColMeta curColMeta;
		for (int i = 0; i < columns.length; i++) {
			curColMeta = toIndexMap.get(columns[i].toUpperCase());
			if (curColMeta == null) {
				throw new java.lang.IllegalArgumentException(
						"group by语句包含的字段必须出现在select语句字段列表里面" + columns[i]);
			}
			result[i] = curColMeta.colIndex;
		}
		return result;
	}

	/**
	 * release resources
	 */
	public void clear() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("clear data ");
		}
		temniated = true;
		grouper = null;
		sorter = null;
		result = null;
		jobQueue.clear();
	}

}

class DataNodeResultInf {
	public RowDataPacket firstRecord;
	public RowDataPacket lastRecord;
	public int rowCount;

}
