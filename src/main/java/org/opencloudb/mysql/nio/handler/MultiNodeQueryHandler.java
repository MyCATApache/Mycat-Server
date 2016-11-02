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
package org.opencloudb.mysql.nio.handler;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.mpp.ColMeta;
import org.opencloudb.mpp.DataMergeService;
import org.opencloudb.mpp.MergeCol;
import org.opencloudb.mysql.LoadDataUtil;
import org.opencloudb.net.mysql.*;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.stat.QueryResult;
import org.opencloudb.stat.QueryResultDispatcher;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements
		LoadDataResponseHandler {
	private static final Logger LOGGER = Logger
			.getLogger(MultiNodeQueryHandler.class);

	private final RouteResultset rrs;
	private final NonBlockingSession session;
	// private final CommitNodeHandler icHandler;
	private final DataMergeService dataMergeSvr;
	private final boolean autocommit;
	private String priamaryKeyTable = null;
	private int primaryKeyIndex = -1;
	private int fieldCount = 0;
	private final ReentrantLock lock;
	private long affectedRows;
	private long insertId;
	private volatile boolean fieldsReturned;
	private int okCount;
	private final boolean isCallProcedure;
	private long startTime;
	private int execCount = 0;

	public MultiNodeQueryHandler(int sqlType, RouteResultset rrs,
			boolean autocommit, NonBlockingSession session) {
		super(session);
		if (rrs.getNodes() == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("execute mutinode query " + rrs.getStatement());
		}
		this.rrs = rrs;
		if (ServerParse.SELECT == sqlType && rrs.needMerge()) {
			dataMergeSvr = new DataMergeService(this, rrs);
		} else {
			dataMergeSvr = null;
		}
		isCallProcedure = rrs.isCallStatement();
		this.autocommit = session.getSource().isAutocommit();
		this.session = session;
		this.lock = new ReentrantLock();
		// this.icHandler = new CommitNodeHandler(session);
		if (dataMergeSvr != null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("has data merge logic ");
			}
		}
	}

	protected void reset(int initCount) {
		super.reset(initCount);
		this.okCount = initCount;
		this.execCount = 0;
	}

	public NonBlockingSession getSession() {
		return session;
	}

	public void execute() throws Exception {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			this.reset(rrs.getNodes().length);
			this.fieldsReturned = false;
			this.affectedRows = 0L;
			this.insertId = 0L;
		} finally {
			lock.unlock();
		}
		MycatConfig conf = MycatServer.getInstance().getConfig();
		startTime = System.currentTimeMillis();
		for (final RouteResultsetNode node : rrs.getNodes()) {
			BackendConnection conn = session.getTarget(node);
			if (session.tryExistsCon(conn, node)) {
				_execute(conn, node);
			} else {
				// create new connection
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), autocommit, node, this, node);
			}

		}
	}

	private void _execute(BackendConnection conn, RouteResultsetNode node) {
		if (clearIfSessionClosed(session)) {
			return;
		}
		conn.setResponseHandler(this);
		try {
			conn.execute(node, session.getSource(), autocommit);
		} catch (IOException e) {
			connectionError(e, conn);
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		final RouteResultsetNode node = (RouteResultsetNode) conn
				.getAttachment();
		session.bindConnection(node, conn);
		_execute(conn, node);
	}

	private boolean decrementOkCountBy(int finished) {
		lock.lock();
		try {
			return --okCount == 0;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("received ok response ,executeResponse:"
					+ executeResponse + " from " + conn);
		}
		if (executeResponse) {

			ServerConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
            //存储过程
            boolean isCanClose2Client =(!rrs.isCallStatement()) ||(rrs.isCallStatement() &&!rrs.getProcedure().isResultSimpleValue());;
             if(!isCallProcedure)
             {
                 if (clearIfSessionClosed(session))
                 {
                     return;
                 } else if (canClose(conn, false))
                 {
                     return;
                 }
             }
			lock.lock();
			try {
				// 判断是否是全局表，如果是，执行行数不做累加，以最后一次执行的为准。
				if (!rrs.isGlobalTable()) {
					affectedRows += ok.affectedRows;
				} else {
					affectedRows = ok.affectedRows;
				}
				if (ok.insertId > 0) {
					insertId = (insertId == 0) ? ok.insertId : Math.min(
							insertId, ok.insertId);
				}
			} finally {
				lock.unlock();
			}

			// 对于存储过程，其比较特殊，查询结果返回EndRow报文以后，还会再返回一个OK报文，才算结束
			boolean isEndPacket = isCallProcedure ? decrementOkCountBy(1)
					: decrementCountBy(1);
			if (isEndPacket&&isCanClose2Client) {
				if (this.autocommit) {// clear all connections
					session.releaseConnections(false);
				}
				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}
				lock.lock();
				try {
					if (rrs.isLoadData()) {
						byte lastPackId = source.getLoadDataInfileHandler()
								.getLastPackId();
						ok.packetId = ++lastPackId;// OK_PACKET
						ok.message = ("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0")
								.getBytes();// 此处信息只是为了控制台给人看的
						source.getLoadDataInfileHandler().clear();
					} else {
						ok.packetId = ++packetId;// OK_PACKET
					}

					ok.affectedRows = affectedRows;
					ok.serverStatus = source.isAutocommit() ? 2 : 1;
					if (insertId > 0) {
						ok.insertId = insertId;
						source.setLastInsertId(insertId);
					}
					ok.write(source);
				} catch (Exception e) {
					handleDataProcessException(e);
				} finally {
					lock.unlock();
				}
			}
		}
	}

	@Override
	public void rowEofResponse(final byte[] eof, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("on row end reseponse " + conn);
		}
		if (errorRepsponsed.get()) {
			// the connection has been closed or set to "txInterrupt" properly
			//in tryErrorFinished() method! If we close it here, it can
			// lead to tx error such as blocking rollback tx for ever.
			// @author Uncle-pan
			// @since 2016-03-25
			// conn.close(this.error);
			return;
		}

		final ServerConnection source = session.getSource();
        if (!rrs.isCallStatement()){
			if (clearIfSessionClosed(session)) {
				return;
			} else if (canClose(conn, false)) {
				return;
			}
		}

		if (decrementCountBy(1)) {
            if (!rrs.isCallStatement()||(rrs.isCallStatement()&&rrs.getProcedure().isResultSimpleValue())) {
				if (this.autocommit) {// clear all connections
					session.releaseConnections(false);
				}

				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}
			}
			if (dataMergeSvr != null) {
				try {
					dataMergeSvr.outputMergeResult(session, eof);
				} catch (Exception e) {
					handleDataProcessException(e);
				}

			} else {
				try {
					lock.lock();
					eof[3] = ++packetId;
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("last packet id:" + packetId);
					}
					source.write(eof);
				} finally {
					lock.unlock();

				}
			}
		}
	}

	public void outputMergeResult(final ServerConnection source,
			final byte[] eof, List<RowDataPacket> results) {
		try {
			lock.lock();
			ByteBuffer buffer = session.getSource().allocate();
			final RouteResultset rrs = this.dataMergeSvr.getRrs();

			// 处理limit语句
			int start = rrs.getLimitStart();
			int end = start + rrs.getLimitSize();

                        if (start < 0)
				start = 0;

			if (rrs.getLimitSize() < 0)
				end = results.size();
				
//			// 对于不需要排序的语句,返回的数据只有rrs.getLimitSize()
//			if (rrs.getOrderByCols() == null) {
//				end = results.size();
//				start = 0;
//			}
			if (end > results.size())
				end = results.size();

			for (int i = start; i < end; i++) {
				RowDataPacket row = results.get(i);
				row.packetId = ++packetId;
				buffer = row.write(buffer, source, true);
			}

			eof[3] = ++packetId;
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("last packet id:" + packetId);
			}
			source.write(source.writeToBuffer(eof, buffer));

		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
			dataMergeSvr.clear();
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		ServerConnection source = null;
		execCount++;
		if (execCount == rrs.getNodes().length) {			
			//TODO: add by zhuam
			//查询结果派发
			QueryResult queryResult = new QueryResult(session.getSource().getUser(), session.getSource().getHost(),
					rrs.getSqlType(), rrs.getStatement(), startTime, System.currentTimeMillis());
			QueryResultDispatcher.dispatchQuery( queryResult );
		}
		if (fieldsReturned) {
			return;
		}
		lock.lock();
		try {
			if (fieldsReturned) {
				return;
			}
			fieldsReturned = true;

			boolean needMerg = (dataMergeSvr != null)
					&& dataMergeSvr.getRrs().needMerge();
			Set<String> shouldRemoveAvgField = new HashSet<>();
			Set<String> shouldRenameAvgField = new HashSet<>();
			if (needMerg) {
				Map<String, Integer> mergeColsMap = dataMergeSvr.getRrs()
						.getMergeCols();
				if (mergeColsMap != null) {
					for (Map.Entry<String, Integer> entry : mergeColsMap
							.entrySet()) {
						String key = entry.getKey();
						int mergeType = entry.getValue();
						if (MergeCol.MERGE_AVG == mergeType
								&& mergeColsMap.containsKey(key + "SUM")) {
							shouldRemoveAvgField.add((key + "COUNT")
									.toUpperCase());
							shouldRenameAvgField.add((key + "SUM")
									.toUpperCase());
						}
					}
				}

			}

			source = session.getSource();
			ByteBuffer buffer = source.allocate();
			fieldCount = fields.size();
			if (shouldRemoveAvgField.size() > 0) {
				ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
				packet.packetId = ++packetId;
				packet.fieldCount = fieldCount - shouldRemoveAvgField.size();
				buffer = packet.write(buffer, source, true);
			} else {

				header[3] = ++packetId;
				buffer = source.writeToBuffer(header, buffer);
			}

			String primaryKey = null;
			if (rrs.hasPrimaryKeyToCache()) {
				String[] items = rrs.getPrimaryKeyItems();
				priamaryKeyTable = items[0];
				primaryKey = items[1];
			}

			Map<String, ColMeta> columToIndx = new HashMap<String, ColMeta>(
					fieldCount);

			for (int i = 0, len = fieldCount; i < len; ++i) {
				boolean shouldSkip = false;
				byte[] field = fields.get(i);
				if (needMerg) {
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					String fieldName = new String(fieldPkg.name).toUpperCase();
					if (columToIndx != null
							&& !columToIndx.containsKey(fieldName)) {
						if (shouldRemoveAvgField.contains(fieldName)) {
							shouldSkip = true;
						}
						if (shouldRenameAvgField.contains(fieldName)) {
							String newFieldName = fieldName.substring(0,
									fieldName.length() - 3);
							fieldPkg.name = newFieldName.getBytes();
							fieldPkg.packetId = ++packetId;
							shouldSkip = true;
							// 处理AVG字段位数和精度, AVG位数 = SUM位数 - 14
							fieldPkg.length = fieldPkg.length - 14;
							// AVG精度 = SUM精度 + 4
 							fieldPkg.decimals = (byte) (fieldPkg.decimals + 4);
							buffer = fieldPkg.write(buffer, source, false);

							// 还原精度
							fieldPkg.decimals = (byte) (fieldPkg.decimals - 4);
						}

						ColMeta colMeta = new ColMeta(i, fieldPkg.type);
						colMeta.decimals = fieldPkg.decimals;
						columToIndx.put(fieldName, colMeta);
					}
				} else if (primaryKey != null && primaryKeyIndex == -1) {
					// find primary key index
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					String fieldName = new String(fieldPkg.name);
					if (primaryKey.equalsIgnoreCase(fieldName)) {
						primaryKeyIndex = i;
						fieldCount = fields.size();
					}
				}
				if (!shouldSkip) {
					field[3] = ++packetId;
					buffer = source.writeToBuffer(field, buffer);
				}
			}
			eof[3] = ++packetId;
			buffer = source.writeToBuffer(eof, buffer);
			source.write(buffer);
			if (dataMergeSvr != null) {
				dataMergeSvr.onRowMetaData(columToIndx, fieldCount);

			}
		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
		}
	}

	public void handleDataProcessException(Exception e) {
		if (!errorRepsponsed.get()) {
			this.error = e.toString();
			LOGGER.warn("caught exception ", e);
			setFail(e.toString());
			this.tryErrorFinished(true);
		}
	}

	@Override
	public void rowResponse(final byte[] row, final BackendConnection conn) {
		if (errorRepsponsed.get()) {
			// the connection has been closed or set to "txInterrupt" properly
			//in tryErrorFinished() method! If we close it here, it can
			// lead to tx error such as blocking rollback tx for ever.
			// @author Uncle-pan
			// @since 2016-03-25
			//conn.close(error);
			return;
		}
		lock.lock();
		try {
			RouteResultsetNode rNode = (RouteResultsetNode) conn
					.getAttachment();
			String dataNode = rNode.getName();
			if (dataMergeSvr != null) {
				// even through discarding the all rest data, we can't
				//close the connection for tx control such as rollback or commit.
				// So the "isClosedByDiscard" variable is unnecessary.
				// @author Uncle-pan
				// @since 2016-03-25
				dataMergeSvr.onNewRecord(dataNode, row);
			} else {
				// cache primaryKey-> dataNode
				if (primaryKeyIndex != -1) {
					RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
					rowDataPkg.read(row);
					String primaryKey = new String(
							rowDataPkg.fieldValues.get(primaryKeyIndex));
					LayerCachePool pool = MycatServer.getInstance()
							.getRouterservice().getTableId2DataNodeCache();
					pool.putIfAbsent(priamaryKeyTable, primaryKey, dataNode);
				}
				row[3] = ++packetId;
				session.getSource().write(row);
			}

		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void clearResources() {
		if (dataMergeSvr != null) {
			dataMergeSvr.clear();
		}
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		LoadDataUtil.requestFileDataResponse(data, conn);
	}

}
