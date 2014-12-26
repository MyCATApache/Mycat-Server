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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.ConnectionMeta;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.mpp.ColMeta;
import org.opencloudb.mpp.DataMergeService;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler {
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
	public MultiNodeQueryHandler(int sqlType, RouteResultset rrs,
			boolean autocommit, NonBlockingSession session) {
		super(session);
		if (rrs.getNodes() == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
        if(LOGGER.isDebugEnabled())
        {
        	LOGGER.debug("execute mutinode query "+rrs.getStatement());
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

	public void execute() throws Exception {
		ServerConnection sc = session.getSource();
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
			if (session.tryExistsCon(conn, node)) {
				_execute(conn, node);
			} else {
				// create new connection
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				ConnectionMeta conMeta = new ConnectionMeta(dn.getDatabase(),
						sc.getCharset(), sc.getCharsetIndex(), autocommit);
				dn.getConnection(conMeta, node, this, node);
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
			ServerConnection source = session.getSource();
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
					ok.packetId = ++packetId;// OK_PACKET
					ok.affectedRows = affectedRows;

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

		final ServerConnection source = session.getSource();
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
				dataMergeSvr.outputMergeResult(session, eof);

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
			final byte[] eof) {
		try {
			lock.lock();
			ByteBuffer buffer = session.getSource().allocate();
			int i = 0;
			int start = dataMergeSvr.getRrs().getLimitStart();
			int end = start + dataMergeSvr.getRrs().getLimitSize();
			Collection<RowDataPacket> results = dataMergeSvr.getResults(eof);
			Iterator<RowDataPacket> itor = results.iterator();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("output merge result ,total data "
						+ results.size() + " start :" + start + " end :" + end
						+ " package id start:" + packetId);
			}
			if (results.size() == dataMergeSvr.getRrs().getLimitSize()) {// 返回的结果只有getLimitSize
				while (itor.hasNext()) {
					RowDataPacket row = itor.next();
					row.packetId = ++packetId;
					buffer = row.write(buffer, source, true);
				}
			} else {
				while (itor.hasNext()) {
					RowDataPacket row = itor.next();
					itor.remove();
					if (i < start) {
						i++;
						continue;
					} else if (i == end) {
						break;
					}
					i++;
					row.packetId = ++packetId;
					buffer = row.write(buffer, source, true);
				}
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
		if (fieldsReturned) {
			return;
		}
		lock.lock();
		try {
			if (fieldsReturned) {
				return;
			}
			fieldsReturned = true;
			
			header[3] = ++packetId;
			source = session.getSource();
			ByteBuffer buffer=source.allocate();
			buffer=source.writeToBuffer(header, buffer);
			fieldCount = fields.size();

			String primaryKey = null;
			if (rrs.hasPrimaryKeyToCache()) {
				String[] items = rrs.getPrimaryKeyItems();
				priamaryKeyTable = items[0];
				primaryKey = items[1];
			}

			Map<String, ColMeta> columToIndx = new HashMap<String, ColMeta>(
					fieldCount);
			boolean needMerg = (dataMergeSvr != null)
					&& dataMergeSvr.getRrs().needMerge();
			for (int i = 0, len = fieldCount; i < len; ++i) {
				byte[] field = fields.get(i);
				if (needMerg) {
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					String fieldName = new String(fieldPkg.name).toUpperCase();
					if (columToIndx != null
							&& !columToIndx.containsKey(fieldName)) {

						columToIndx.put(fieldName,
								new ColMeta(i, fieldPkg.type));
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

				field[3] = ++packetId;
				buffer=source.writeToBuffer(field, buffer);
			}
			eof[3] = ++packetId;
			buffer=source.writeToBuffer(eof, buffer);
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

	private void handleDataProcessException(Exception e) {
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
	public void writeQueueAvailable() {

	}

}