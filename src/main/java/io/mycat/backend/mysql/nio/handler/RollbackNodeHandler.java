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

import java.util.List;

import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.OkPacket;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

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
		int start = 0 ;
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
//					break;
				} else {
					start ++;
				}
			}
		}
		//有连接已被关闭 直接关闭所有的连接， 非xa模式下 ，xa prepare状态下需要rollback
		if(hasClose && session.getXaTXID() == null) {
			LOGGER.warn("find close back conn close ,so close all back connection"
					+ session.getSource());
			session.setAutoCommitStatus(); //一定是先恢复状态 在写消息
			this.setFail("receive rollback,but find backend con is closed or quit");
			this.tryErrorFinished(true);

			return ;
		}
		
		// 执行
		//modify by zwy
		
		// 执行
		lock.lock();
		try {
			reset(start);
		} finally {
			lock.unlock();
		}
		boolean writeCheckPoint = false;
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
				    	this.setFail("receive rollback,but find backend con is closed or quit");
					//	session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR,
					//			"receive rollback,but find backend con is closed or quit");
						LOGGER.error( conn+"receive rollback,but fond backend con is closed or quit");
						continue;
					}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("rollback job run for " + conn);
				}
				if (clearIfSessionClosed(session)) {
					return;
				}
				conn.setResponseHandler(RollbackNodeHandler.this);

				//support the XA rollback 
				//to do to write xa recover log and judge xa statue to judge if send xa end 
				if(session.getXaTXID()!=null && conn instanceof  MySQLConnection) {
					MySQLConnection mysqlCon = (MySQLConnection) conn;
					
					//recovery log
					String xaTxId = session.getXaTXID();
					CoordinatorLogEntry coordinatorLogEntry = MultiNodeCoordinator.inMemoryRepository.get(xaTxId);
					if(coordinatorLogEntry != null) {
						writeCheckPoint = true;
						//已经prepare的修改recover log
						for(int i=0; i<coordinatorLogEntry.participants.length;i++){
							if(coordinatorLogEntry.participants[i].resourceName.equals(conn.getSchema())){
								coordinatorLogEntry.participants[i].txState = TxState.TX_ROLLBACKED_STATE;
							}
						}
						MultiNodeCoordinator.inMemoryRepository.put(xaTxId,coordinatorLogEntry);
					}
					
					xaTxId = session.getXaTXID() +",'"+ mysqlCon.getSchema()+"'";
					//exeBatch cmd issue : the 2nd package can not receive the response
//					mysqlCon.execCmd("XA END " + xaTxId  + ";");
//					mysqlCon.execCmd("XA ROLLBACK " + xaTxId + ";");		
					if(mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE ){
						 String[] cmds = new String[]{"XA END " + xaTxId  + "",
								 " XA ROLLBACK " + xaTxId + ";"};
						   mysqlCon.execBatchCmd(cmds);
					} else if(mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE ){						
						 String[] cmds = new String[]{" XA ROLLBACK " + xaTxId + ";"};						   
						 mysqlCon.execBatchCmd(cmds);
					} else {
						LOGGER.warn("{} xaStat is {} ,to rollback is error" ,mysqlCon, mysqlCon.getXaStatus());
					}			
					mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
				}else {
					//modify by zwy
					if(!conn.isClosedOrQuit()) {
						conn.rollback();
						//++started;
					}
				}
			}
		}
		if(writeCheckPoint) {
			MultiNodeCoordinator.fileRepository.writeCheckpoint(session.getXaTXID(), MultiNodeCoordinator.inMemoryRepository.getAllCoordinatorLogEntries());

		}

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		
		if(session.getXaTXID()!=null) {
			//xa 
			if( conn instanceof  MySQLConnection) {
				MySQLConnection mysqlCon = (MySQLConnection) conn;
				if (!mysqlCon.batchCmdFinished()) { 
					// 
					return;
				}
			}			
		}		
		if (decrementCountBy(1)) {
			// clear all resources
			session.clearResources(false);
			//回复之前的事务状态 by zhangwy 2018.07
			session.setAutoCommitStatus();
			if (this.isFail() || session.closed()) {
				tryErrorFinished(true);
			} else {
		        if(session.getSource().canResponse()) {
		        	OkPacket okPacket = new OkPacket();
		        	okPacket.read(ok);
		        	okPacket.packetId = 1;
					session.getSource().write(okPacket.writeToBytes());
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
