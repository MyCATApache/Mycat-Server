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

import io.mycat.backend.BackendConnection;
import io.mycat.backend.nio.MySQLBackendConnection;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.packet.ErrorPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author mycat
 */
public class CommitNodeHandler implements ResponseHandler {
	public static final Logger LOGGER = LoggerFactory
			.getLogger(CommitNodeHandler.class);
	private final NonBlockingSession session;

	public CommitNodeHandler(NonBlockingSession session) {
		this.session = session;
	}

	public void commit(BackendConnection conn) {
		conn.setResponseHandler(CommitNodeHandler.this);
		if (conn instanceof MySQLBackendConnection) {
			MySQLBackendConnection mysqlCon = (MySQLBackendConnection) conn;
			if (mysqlCon.getXaStatus() == 1) {
				String xaTxId = session.getXaTXID();
				String[] cmds = new String[] { "XA END " + xaTxId,
						"XA PREPARE " + xaTxId };
				mysqlCon.execBatchCmd(cmds);
			} else {
				conn.commit();
			}
		} else {
			conn.commit();
		}
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		LOGGER.error("unexpected invocation: connectionAcquired from commit");

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (conn instanceof MySQLBackendConnection) {
			MySQLBackendConnection mysqlCon = (MySQLBackendConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			case 1:
				if (mysqlCon.batchCmdFinished()) {
					String xaTxId = session.getXaTXID();
					mysqlCon.execCmd("XA COMMIT " + xaTxId);
					mysqlCon.setXaStatus(2);
				}
				return;
			case 2: {
				mysqlCon.setXaStatus(0);
				break;
			}
			}
		}
		session.clearResources(false);
		session.getSource().write(ok);

	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		ErrorPacket errPkg = new ErrorPacket();
		errPkg.read(err);
		String errInfo = new String(errPkg.message);
		session.getSource().setTxInterrupt(errInfo);
		errPkg.write(session.getSource());

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
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
		LOGGER.warn(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": row data packet").toString());
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {

	}

}