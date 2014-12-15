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

import java.nio.ByteBuffer;
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
import org.opencloudb.mpp.MutiDataMergeService;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;

/**
 * @author mycat
 */
public class MultiNodeQueryWithLimitHandler extends MultiNodeQueryHandler {
	private static final Logger LOGGER = Logger
			.getLogger(MultiNodeQueryWithLimitHandler.class);

	private final RouteResultset rrs;
	private final NonBlockingSession session;
	// private final CommitNodeHandler icHandler;
	private final MutiDataMergeService dataMergeSvr;
	private volatile boolean mergeOutputed;
	private final boolean autocommit;
	private String priamaryKeyTable = null;
	private int primaryKeyIndex = -1;
	private int fieldCount = 0;
	private final ReentrantLock lock;
	private long affectedRows;
	private long insertId;
	private boolean fieldsReturned;

	public MultiNodeQueryWithLimitHandler(RouteResultset rrs,
			boolean autocommit, NonBlockingSession session,
			MutiDataMergeService dataMergeSvr) {
		super(rrs, autocommit, session, dataMergeSvr);
		if (rrs.getNodes() == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		this.rrs = rrs;
		this.rrs.resetNodes();

		this.autocommit = session.getSource().isAutocommit();
		this.session = session;
		this.lock = new ReentrantLock();
		// this.icHandler = new CommitNodeHandler(session);
		this.dataMergeSvr = dataMergeSvr;
		if (this.dataMergeSvr != null) {
			this.dataMergeSvr.setLimitExcution(this);
			this.dataMergeSvr.initHandler(session);

		}
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
		this.dataMergeSvr.execute(conn, node);
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		final RouteResultsetNode node = (RouteResultsetNode) conn
				.getAttachment();
		session.bindConnection(node, conn);
		session.getSource().getProcessor().getExecutor()
				.execute(new Runnable() {
					@Override
					public void run() {
						_execute(conn, node);
					}
				});
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		String errmsg = new String(err.message);
		LOGGER.warn("error response from backend, code:" + err.errno
				+ " errmsg: " + errmsg + ",from " + conn);
		if (this.errorRepsponsed.get()) {
			conn.close(error);
			return;
		}
		this.setFail(errmsg);
		// try connection and finish conditon check
		canClose(conn, true);
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
				affectedRows += ok.affectedRows;
				if (ok.insertId > 0) {
					insertId = (insertId == 0) ? ok.insertId : Math.min(
							insertId, ok.insertId);
				}
			} finally {
				lock.unlock();
			}
			if (decrementCountBy(1)) {
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
					handleDataProcessException(e, conn);
				} finally {
					lock.unlock();
				}
			}
		}
	}

	protected boolean canClose(BackendConnection conn, boolean tryErrorFinish) {
		if (conn == null) {
			this.dataMergeSvr.releaseAllBackend();
		}

		boolean allFinished = false;
		if (tryErrorFinish) {
			allFinished = this.decrementCountBy(1);
			this.tryErrorFinished(allFinished);
		}

		return allFinished;
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("on row end reseponse " + conn);
		}
		if (errorRepsponsed.get()) {
			conn.close(error);
			return;
		}
		if (clearIfSessionClosed(session)) {
			return;
		} else if (canClose(conn, false)) {
			return;
		}
		ServerConnection source = session.getSource();
		if (this.autocommit) {// clear all connections
			session.releaseConnections(false);
		}

		if (this.isFail() || session.closed()) {
			tryErrorFinished(true);
			return;
		}

		try {
			lock.lock();
			ByteBuffer buffer = session.getSource().allocate();

			if (dataMergeSvr != null && !mergeOutputed) {
				int i = 0;
				int start = dataMergeSvr.getRrs().getLimitStart();
				start -= this.dataMergeSvr.loadTrimTotal();
				int end = start + dataMergeSvr.getRrs().getLimitSize();
				Iterator<RowDataPacket> itor = dataMergeSvr.getResults()
						.iterator();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("output merge result ,total data "
							+ dataMergeSvr.getResults().size() + " start :"
							+ start + " end :" + end + " package id start:"
							+ packetId);
				}
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
			handleDataProcessException(e, conn);
		} finally {
			lock.unlock();
			if (dataMergeSvr != null) {
				dataMergeSvr.clear();
			}
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		ServerConnection source = null;
		lock.lock();

		try {
			if (fieldsReturned) {
				return;
			}
			fieldsReturned = true;
			header[3] = ++packetId;
			source = session.getSource();

			ByteBuffer buffer = session.getSource().allocate();
			buffer = source.writeToBuffer(header, buffer);
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
				source.write(field);
			}
			if (dataMergeSvr != null) {
				dataMergeSvr.onRowMetaData(columToIndx, fieldCount);

			}

			eof[3] = ++packetId;
			source.write(eof);
		} catch (Exception e) {
			handleDataProcessException(e, conn);
		} finally {
			lock.unlock();
		}
	}

	private void handleDataProcessException(Exception e, BackendConnection conn) {
		if (!errorRepsponsed.get()) {
			conn.close(error);
			LOGGER.warn("caught exception ", e);
			setFail(e.toString());
			this.tryErrorFinished(true);
		}
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		if (errorRepsponsed.get()) {
			conn.close(error);
			return;
		}
		lock.lock();
		try {
			if (dataMergeSvr != null) {
				boolean canOutput = dataMergeSvr.onNewRecord(
						((RouteResultsetNode) conn.getAttachment()).getName(),
						row);
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
			handleDataProcessException(e, conn);
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