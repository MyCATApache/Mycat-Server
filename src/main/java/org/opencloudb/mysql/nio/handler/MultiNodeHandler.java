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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
abstract class MultiNodeHandler implements ResponseHandler, Terminatable {
	private static final Logger LOGGER = Logger
			.getLogger(MultiNodeHandler.class);
	protected final ReentrantLock lock = new ReentrantLock();
	protected final NonBlockingSession session;
	private AtomicBoolean isFailed = new AtomicBoolean(false);
	protected volatile String error;
	protected byte packetId;
	protected final AtomicBoolean errorRepsponsed = new AtomicBoolean(false);
	
	public MultiNodeHandler(NonBlockingSession session) {
		if (session == null) {
			throw new IllegalArgumentException("session is null!");
		}
		this.session = session;
	}

	public void setFail(String errMsg) {
		isFailed.set(true);
		error = errMsg;
	}

	public boolean isFail() {
		return isFailed.get();
	}

	private int nodeCount;

	private Runnable terminateCallBack;

	@Override
	public void terminate(Runnable terminateCallBack) {
		boolean zeroReached = false;
		lock.lock();
		try {
			if (nodeCount > 0) {
				this.terminateCallBack = terminateCallBack;
			} else {
				zeroReached = true;
			}
		} finally {
			lock.unlock();
		}
		if (zeroReached) {
			terminateCallBack.run();
		}
	}

	protected boolean canClose(BackendConnection conn, boolean tryErrorFinish) {

		// realse this connection if safe
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
		boolean allFinished = false;
		if (tryErrorFinish) {
			allFinished = this.decrementCountBy(1);
			this.tryErrorFinished(allFinished);
		}

		return allFinished;
	}

	protected void decrementCountToZero() {
		Runnable callback;
		lock.lock();
		try {
			nodeCount = 0;
			callback = this.terminateCallBack;
			this.terminateCallBack = null;
		} finally {
			lock.unlock();
		}
		if (callback != null) {
			callback.run();
		}
	}

	public void connectionError(Throwable e, BackendConnection conn) {
		final boolean canClose = decrementCountBy(1);
		// 需要把Throwable e的错误信息保存下来（setFail()）， 否则会导致响应 
		//null信息，结果mysql命令行等客户端查询结果是"Query OK"！！
		// @author Uncle-pan
		// @since 2016-03-26
		if(canClose){
			setFail("backend connect: "+e);
		}
		LOGGER.warn("backend connect", e);
		this.tryErrorFinished(canClose);
	}

	public void errorResponse(byte[] data, BackendConnection conn) {
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		String errmsg = new String(err.message);
		this.setFail(errmsg);
		LOGGER.warn("error response from " + conn + " err " + errmsg + " code:"
				+ err.errno);

		this.tryErrorFinished(this.decrementCountBy(1));
	}

	public boolean clearIfSessionClosed(NonBlockingSession session) {
		if (session.closed()) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("session closed ,clear resources " + session);
			}

			session.clearResources(true);
			this.clearResources();
			return true;
		} else {
			return false;
		}

	}

	protected boolean decrementCountBy(int finished) {
		boolean zeroReached = false;
		Runnable callback = null;
		lock.lock();
		try {
			if (zeroReached = --nodeCount == 0) {
				callback = this.terminateCallBack;
				this.terminateCallBack = null;
			}
		} finally {
			lock.unlock();
		}
		if (zeroReached && callback != null) {
			callback.run();
		}
		return zeroReached;
	}

	protected void reset(int initCount) {
		nodeCount = initCount;
		isFailed.set(false);
		error = null;
		packetId = 0;
	}

	protected ErrorPacket createErrPkg(String errmgs) {
		ErrorPacket err = new ErrorPacket();
		lock.lock();
		try {
			err.packetId = ++packetId;
		} finally {
			lock.unlock();
		}
		err.errno = ErrorCode.ER_UNKNOWN_ERROR;
		err.message = StringUtil.encode(errmgs, session.getSource()
				.getCharset());
		return err;
	}

	protected void tryErrorFinished(boolean allEnd) {
		if (allEnd && !session.closed()) {
			if (errorRepsponsed.compareAndSet(false, true)) {
				createErrPkg(this.error).write(session.getSource());
			}
			// clear session resources,release all
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("error all end ,clear session resource ");
			}
			if (session.getSource().isAutocommit()) {
				session.closeAndClearResources(error);
			} else {
				session.getSource().setTxInterrupt(this.error);
				// clear resouces
				clearResources();
			}

		}

	}

	public void connectionClose(BackendConnection conn, String reason) {
		this.setFail("closed connection:" + reason + " con:" + conn);
		boolean finished = false;
		lock.lock();
		try {
			finished = (this.nodeCount == 0);

		} finally {
			lock.unlock();
		}
		if (finished == false) {
			finished = this.decrementCountBy(1);
		}
		if (error == null) {
			error = "back connection closed ";
		}
		tryErrorFinished(finished);
	}

	public void clearResources() {
	}
}