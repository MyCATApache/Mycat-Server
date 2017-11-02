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
package io.mycat.server.executors;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.backend.nio.MySQLBackendConnection;
import io.mycat.cache.LayerCachePool;
import io.mycat.net.BufferArray;
import io.mycat.net.NetSystem;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.packet.BinaryRowDataPacket;
import io.mycat.server.config.node.MycatConfig;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.OkPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.server.packet.util.LoadDataUtil;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.ColMeta;
import io.mycat.sqlengine.mpp.DataMergeService;
import io.mycat.sqlengine.mpp.MergeCol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements
		LoadDataResponseHandler {
	public static final Logger LOGGER = LoggerFactory
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
	private boolean prepared;
	private List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();

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

		for (final RouteResultsetNode node : rrs.getNodes()) {
			final BackendConnection conn = session.getTarget(node);
			
			node.setRunOnSlave(rrs.getRunOnSlave());
			
//			// 强制走 master, session.tryExistsCon 会用到属性canRunInReadDB进行判断；
//			// 防止重用了 slave 的连接去走master
//			if(rrs.getRunOnSlave() != null && !rrs.getRunOnSlave())
//				node.setCanRunInReadDB(false); // 保证不会重用到slave的连接
			
			if (session.tryExistsCon(conn, node)) {
				LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
				_execute(conn, node);
			} else {
				// create new connection
				LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
				
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), autocommit, node, this, node);
				// 注意该方法不仅仅是获取连接，获取新连接成功之后，会通过层层回调，最后回调到本类 的connectionAcquired
				// 这是通过 上面方法的 this 参数的层层传递完成的。
				// connectionAcquired 进行执行操作:
				// session.bindConnection(node, conn);
				// _execute(conn, node);
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
			if (clearIfSessionClosed(session)) {
				return;
			} else if (canClose(conn, false)) {
				return;
			}
			MySQLFrontConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
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
			if (isEndPacket) {
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
			conn.close(this.error);
			return;
		}

		final MySQLFrontConnection source = session.getSource();
		if (!isCallProcedure) {
			if (clearIfSessionClosed(session)) {
				return;
			} else if (canClose(conn, false)) {
				return;
			}
		}

		if (decrementCountBy(1)) {
			if (!this.isCallProcedure) {
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

	public void outputMergeResult(final MySQLFrontConnection source,
			final byte[] eof) {
		try {
			lock.lock();
			BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
					.allocateArray();
			final DataMergeService dataMergeService = this.dataMergeSvr;
			final RouteResultset rrs = dataMergeService.getRrs();

			// 处理limit语句
			int start = rrs.getLimitStart();
			int end = start + rrs.getLimitSize();

			/*
			 * modify by coder_czp@126.com#2015/11/2 优化为通过索引获取,避免无效循环
			 * Collection<RowDataPacket> results = dataMergeSvr.getResults(eof);
			 * Iterator<RowDataPacket> itor = results.iterator(); if
			 * (LOGGER.isDebugEnabled()) {
			 * LOGGER.debug("output merge result ,total data " + results.size()
			 * + " start :" + start + " end :" + end + " package id start:" +
			 * packetId); } int i = 0; while (itor.hasNext()) { RowDataPacket
			 * row = itor.next(); if (i < start) { i++; continue; } else if (i
			 * == end) { break; } i++; row.packetId = ++packetId; buffer =
			 * row.write(buffer, source, true); }
			 */
			// 对于不需要排序的语句,返回的数据只有rrs.getLimitSize()
			List<RowDataPacket> results = dataMergeSvr.getResults(eof);
            if (start < 0)
               			start = 0;
            if(rrs.getLimitSize()<0 || end > results.size())
            {
                end=results.size();
            }
//			if (rrs.getOrderByCols() == null) {
//				end = results.size();
//				start = 0;
//			}
			for (int i = start; i < end; i++) {
				RowDataPacket row = results.get(i);
				if(prepared) {
					BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
					binRowDataPk.read(fieldPackets, row);
					binRowDataPk.packetId = ++packetId;
					binRowDataPk.write(bufferArray);
				} else {
					row.packetId = ++packetId;
					row.write(bufferArray);
				}
			}

			eof[3] = ++packetId;
			bufferArray.write(eof);
			source.write(bufferArray);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("last packet id:" + packetId);
			}

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
		MySQLFrontConnection source = null;
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
			BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
					.allocateArray();
			fieldCount = fields.size();
			if (shouldRemoveAvgField.size() > 0) {
				ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
				packet.packetId = ++packetId;
				packet.fieldCount = fieldCount - shouldRemoveAvgField.size();
				packet.write(bufferArray);
			} else {
				header[3] = ++packetId;
				bufferArray.write(header);
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
					fieldPackets.add(fieldPkg);
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
							fieldPkg.write(bufferArray);
						}

						columToIndx.put(fieldName,
								new ColMeta(i, fieldPkg.type));
					}
				} else if (primaryKey != null && primaryKeyIndex == -1) {
					// find primary key index
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					fieldPackets.add(fieldPkg);
					String fieldName = new String(fieldPkg.name);
					if (primaryKey.equalsIgnoreCase(fieldName)) {
						primaryKeyIndex = i;
						fieldCount = fields.size();
					}
				}
				if (!shouldSkip) {
					field[3] = ++packetId;
					bufferArray.write(field);
				}
			}
			eof[3] = ++packetId;
			bufferArray.write(eof);
			source.write(bufferArray);
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
			conn.close(error);
			return;
		}
		lock.lock();
		try {
			if (dataMergeSvr != null) {
				final String dnName = ((RouteResultsetNode) conn
						.getAttachment()).getName();
				dataMergeSvr.onNewRecord(dnName, row);

			} else {
				if (primaryKeyIndex != -1) {// cache
											// primaryKey->
											// dataNode
					RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
					rowDataPkg.read(row);
					String primaryKey = new String(
							rowDataPkg.fieldValues.get(primaryKeyIndex));
					LayerCachePool pool = MycatServer.getInstance()
							.getRouterservice().getTableId2DataNodeCache();
					String dataNode = ((RouteResultsetNode) conn
							.getAttachment()).getName();
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
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		LoadDataUtil.requestFileDataResponse(data,
				(MySQLBackendConnection) conn);
	}

	public boolean isPrepared() {
		return prepared;
	}

	public void setPrepared(boolean prepared) {
		this.prepared = prepared;
	}

}
