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
import java.util.List;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.mpp.MutiDataMergeService;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
public class NodeWithLimitHandler implements ResponseHandler, Terminatable {
	private static final Logger LOGGER = Logger
			.getLogger(NodeWithLimitHandler.class);
	private final RouteResultsetNode node;
	private final NonBlockingSession session;
	private final MutiDataMergeService dataMergeSvr;
	
	// only one thread access at one time no need lock
	private volatile byte packetId;
	private volatile ByteBuffer buffer;
	private volatile boolean isRunning;
	private Runnable terminateCallBack;

	public NodeWithLimitHandler(RouteResultsetNode route,
			NonBlockingSession session, MutiDataMergeService dataMergeSvr) {
		if (route == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		if (session == null) {
			throw new IllegalArgumentException("session is null!");
		}
		if (dataMergeSvr == null) {
			throw new IllegalArgumentException("dataMergeSvr is null!");
		}
		this.session = session;
		this.node = route;
		this.dataMergeSvr = dataMergeSvr;
	}

	@Override
	public void terminate(Runnable callback) {
		boolean zeroReached = false;

		if (isRunning) {
			terminateCallBack = callback;
		} else {
			zeroReached = true;
		}

		if (zeroReached) {
			callback.run();
		}
	}

	private void endRunning() {
		Runnable callback = null;
		if (isRunning) {
			isRunning = false;
			callback = terminateCallBack;
			terminateCallBack = null;
		}

		if (callback != null) {
			callback.run();
		}
	}

	private void recycleResources() {

		ByteBuffer buf = buffer;
		if (buf != null) {
			session.getSource().recycle(buffer);
			buffer = null;
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		session.bindConnection(node, conn);
		session.getSource().getProcessor().getExecutor()
				.execute(new Runnable() {
					@Override
					public void run() {
						_execute(conn);
					}
				});
	}

	private void _execute(BackendConnection conn) {
		if (session.closed()) {
			endRunning();
			session.clearResources(false);
			return;
		}
		conn.setResponseHandler(this);
		try {
			conn.execute(node, session.getSource(), session.getSource()
					.isAutocommit());
		} catch (IOException e1) {
			executeException(conn);
			return;
		}
	}

	private void executeException(BackendConnection c) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode(
				"unknown backend charset: " + c.getCharset(), session
						.getSource().getCharset());

		this.backConnectionErr(err, c);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		endRunning();
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode(e.getMessage(), session.getSource()
				.getCharset());
		ServerConnection source = session.getSource();
		source.write(err.write(allocBuffer(), source, true));
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		err.packetId = ++packetId;
		backConnectionErr(err, conn);

	}

	private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
		endRunning();
		String errmgs=  " errno:" + errPkg.errno +" "+new String(errPkg.message) ;
		LOGGER.warn("execute  sql err :"+errmgs+ " con:" + conn);
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),false);
		ServerConnection source = session.getSource();
		source.setTxInterrupt(errmgs);
		errPkg.write(source);
		recycleResources();
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),false);
			endRunning();
			ServerConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
			ok.packetId = ++packetId;
			recycleResources();
			source.setLastInsertId(ok.insertId);
			ok.write(source);

		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.info("node=" + node + ", row receive. ");
		ServerConnection source = session.getSource();
		//conn.setRunning(false);
		conn.recordSql(source.getHost(), source.getSchema(),
				node.getStatement());
		//session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled());
		//endRunning();
		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, allocBuffer());
		//source.write(buffer);
		this.dataMergeSvr.dataOk(node.getName(), eof, conn);
	}

	/**
	 * lazy create ByteBuffer only when needed
	 * 
	 * @return
	 */
	private ByteBuffer allocBuffer() {
		if (buffer == null) {
			buffer = session.getSource().allocate();
		}
		return buffer;
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		this.dataMergeSvr.fieldEofResponse(header, fields, eof, conn);
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		dataMergeSvr.onNewRecord(this.node.getName(), row);
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode(reason, session.getSource()
				.getCharset());
		this.backConnectionErr(err, conn);

	}

	public void clearResources() {

	}

	@Override
	public String toString() {
		return "NodeWithLimitHandler [node=" + node + ", packetId=" + packetId
				+ "]";
	}

}