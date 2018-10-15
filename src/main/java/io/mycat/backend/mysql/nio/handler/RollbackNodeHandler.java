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
package io.mycat.backend.mysql.nio.handler;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.config.ErrorCode;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author mycat
 */
public class RollbackNodeHandler extends MultiNodeHandler {
	private static final Logger LOGGER = LoggerFactory
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
		
		boolean hasClose = false;
		for (final RouteResultsetNode node : session.getTargetKeys()) {
			if (node == null) {
				LOGGER.error("null is contained in RoutResultsetNodes, source = "
						+ session.getSource());
				hasClose = true;
				break;
			}
			final BackendConnection conn = session.getTarget(node);
			if (conn != null) {
				boolean isClosed=conn.isClosedOrQuit();
				if(isClosed) {
					hasClose = true;
					break;
				}
			}
		}
		//有连接已被关闭 直接关闭所有的连接
		if(hasClose ) {
			LOGGER.warn("find close back conn close ,so close all back connection"
					+ session.getSource());
			session.setAutoCommitStatus(); //一定是先恢复状态 在写消息
			this.setFail("receive rollback,but find backend con is closed or quit");
			this.tryErrorFinished(true);

			return ;
		}
		
		// 执行
		//modify by zwy
//		int closeCount = 0;
		
		// 执行
	//	int started = 0;
		for (final RouteResultsetNode node : session.getTargetKeys()) {
			if (node == null) {
					LOGGER.error("null is contained in RoutResultsetNodes, source = "
							+ session.getSource());
				continue;
			}
			final BackendConnection conn = session.getTarget(node);

			if (conn != null) {
				boolean isClosed=conn.isClosedOrQuit();
				    if(isClosed)
					{
					//	session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR,
					//			"receive rollback,but find backend con is closed or quit");
						LOGGER.error( conn+"receive rollback,but fond backend con is closed or quit");
					}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("rollback job run for " + conn);
				}
				if (clearIfSessionClosed(session)) {
					return;
				}
				conn.setResponseHandler(RollbackNodeHandler.this);

				//support the XA rollback
				if(session.getXaTXID()!=null && conn instanceof  MySQLConnection) {
					MySQLConnection mysqlCon = (MySQLConnection) conn;
					String xaTxId = session.getXaTXID() +",'"+ mysqlCon.getSchema()+"'";
					//exeBatch cmd issue : the 2nd package can not receive the response
					mysqlCon.execCmd("XA END " + xaTxId  + ";");
					mysqlCon.execCmd("XA ROLLBACK " + xaTxId + ";");
				}else {
					//modify by zwy
					if(!conn.isClosedOrQuit()) {
						conn.rollback();
						//++started;
					}
				}


				//++started;
			}
		}

//		if (started < initCount && decrementCountBy(initCount - started)) {
//			/**
//			 * assumption: only caused by front-end connection close. <br/>
//			 * Otherwise, packet must be returned to front-end
//			 */
//			session.clearResources(true);
//		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (decrementCountBy(1)) {
			// clear all resources
			session.clearResources(false);
			//回复之前的事务状态 by zhangwy 2018.07
			session.setAutoCommitStatus();
			if (this.isFail() || session.closed()) {
				tryErrorFinished(true);
			} else {

		        if(session.getSource().canResponse()) {
					session.getSource().write(ok);
				}
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

	public void connectionClose(BackendConnection conn, String reason) {
		this.setFail("closed connection:" + reason + " con:" + conn);
		boolean finished = false;

		if (finished == false) {
			finished = this.decrementCountBy(1);
		}
		if (error == null) {
			error = "back connection closed ";
		}
		if(finished) {
			session.setAutoCommitStatus();
			tryErrorFinished(finished);

		}
	}
	protected void tryErrorFinished(boolean allEnd) {
		if (allEnd && !session.closed()) {
			
			
			
			// clear session resources,release all
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("error all end ,clear session resource ");
			}
			//关闭所有的错误后端连接 清理资源
			session.closeAndClearResources(error);
			//避免高并发 重新在清空一次
			session.getSource().clearTxInterrupt();
			//createErrPkg(this.error).write(session.getSource());
			session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, this.error);
		}

	}
}
