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

import java.util.List;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;

/**
 * @author mycat
 */
public class RollbackNodeHandler extends MultiNodeHandler {
	private static final Logger LOGGER = Logger
			.getLogger(RollbackNodeHandler.class);

	public RollbackNodeHandler(NonBlockingSession session) {
		super(session);
	}

	public void rollback() {
		final int initCount = session.getTargetCount();
		lock.lock();
		try {
			reset(initCount);
		} finally {
			lock.unlock();
		}
		if (session.closed()) {
			decrementCountToZero();
			return;
		}

		// 执行
		int started = 0;
		for (final RouteResultsetNode node : session.getTargetKeys()) {
			if (node == null) {
				try {
					LOGGER.error("null is contained in RoutResultsetNodes, source = "
							+ session.getSource());
				} catch (Exception e) {
				}
				continue;
			}
			final BackendConnection conn = session.getTarget(node);
			if (conn != null) {
				boolean isClosed=conn.isClosedOrQuit();
				if(isClosed)
				{
					session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR,
							"receive rollback,but find backend con is closed or quit");
					LOGGER.error( conn+"receive rollback,but fond backend con is closed or quit");
				}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("rollback job run for " + conn);
				}
				if (clearIfSessionClosed(session)) {
					return;
				}
				conn.setResponseHandler(RollbackNodeHandler.this);
				conn.rollback();

				++started;
			}
		}

		if (started < initCount && decrementCountBy(initCount - started)) {
			/**
			 * assumption: only caused by front-end connection close. <br/>
			 * Otherwise, packet must be returned to front-end
			 */
			session.clearResources(true);
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (decrementCountBy(1)) {
			// clear all resources
			session.clearResources(false);
			if (this.isFail() || session.closed()) {
				tryErrorFinished(true);
			} else {
				session.getSource().write(ok);
			}
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		LOGGER.error("unexpected invocation: connectionAcquired from rollback");
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void writeQueueAvailable() {

	}

}
