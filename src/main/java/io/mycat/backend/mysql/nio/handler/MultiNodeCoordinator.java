package io.mycat.backend.mysql.nio.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.ParticipantLogEntry;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.backend.mysql.xa.recovery.impl.FileSystemRepository;
import io.mycat.backend.mysql.xa.recovery.impl.InMemoryRepository;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.sqlcmd.SQLCtrlCommand;

public class MultiNodeCoordinator implements ResponseHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MultiNodeCoordinator.class);
	public static final Repository fileRepository = new FileSystemRepository();
	public static final Repository inMemoryRepository = new InMemoryRepository();
	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final AtomicInteger prepareCount = new AtomicInteger(0);
	protected volatile int coorXaStatus;

	private final AtomicInteger faileCount = new AtomicInteger(0);
	private volatile int nodeCount;
	private final NonBlockingSession session;
	private SQLCtrlCommand cmdHandler;
	private final AtomicBoolean failed = new AtomicBoolean(false);
	protected volatile String error;
    
	public MultiNodeCoordinator(NonBlockingSession session) {
		this.session = session;
	}

	/** Multi-nodes 1pc Commit Handle **/
	public void executeBatchNodeCmd(SQLCtrlCommand cmdHandler) {
		this.cmdHandler = cmdHandler;
		final int initCount = session.getTargetCount();
		runningCount.set(initCount);
		nodeCount = initCount;
		failed.set(false);
		faileCount.set(0);
		//XA statue init
		if(prepareCount.get() != 0){
			//System.out.println("prepareCount :" + prepareCount.get());
		}
		prepareCount.set(0);
		if(session.getXaTXID()!=null){
			coorXaStatus = TxState.TX_STARTED_STATE;
			writeRecoverLog(initCount); 
		} else {
			coorXaStatus = -1;
		}
		
		// 执行
		int started = 0;
		for (RouteResultsetNode rrn : session.getTargetKeys()) {
			if (rrn == null) {
				LOGGER.error("null is contained in RoutResultsetNodes, source = "
						+ session.getSource());
				continue;
			}
			final BackendConnection conn = session.getTarget(rrn);
			if (conn != null) {
				conn.setResponseHandler(this);
				//process the XA_END XA_PREPARE Command
				if(conn instanceof MySQLConnection){
					MySQLConnection mysqlCon = (MySQLConnection) conn;
					String xaTxId = null;
					if(session.getXaTXID()!=null){
						xaTxId = session.getXaTXID() +",'"+ mysqlCon.getSchema()+"'";
					}
					if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE)
					{
						//recovery Log
//						participantLogEntry[started] = new ParticipantLogEntry(xaTxId,conn.getHost(),0,conn.getSchema(),((MySQLConnection) conn).getXaStatus());
						String[] cmds = new String[]{"XA END " + xaTxId,
								"XA PREPARE " + xaTxId};
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Start execute the batch cmd : "+ cmds[0] + ";" + cmds[1]+","+
									"current connection:"+conn.getHost()+":"+conn.getPort());
						}
//						prepareCount.incrementAndGet();
						mysqlCon.execBatchCmd(cmds);
					} else
					{
						//recovery Log
//						participantLogEntry[started] = new ParticipantLogEntry(xaTxId,conn.getHost(),0,conn.getSchema(),((MySQLConnection) conn).getXaStatus());
						cmdHandler.sendCommand(session, conn);
					}
				}else{
					cmdHandler.sendCommand(session, conn);
				}
				++started;
			}
		}

		
		if (started < nodeCount) {
			runningCount.set(started);
			LOGGER.warn("some connection failed to execute "
					+ (nodeCount - started));
			/**
			 * assumption: only caused by front-end connection close. <br/>
			 * Otherwise, packet must be returned to front-end
			 */
			failed.set(true);
		}
	}

	private void writeRecoverLog(int initCount) {
		// 执行
		int started = 0;
		//recovery nodes log
		ParticipantLogEntry[] participantLogEntry = new ParticipantLogEntry[initCount];
		for (RouteResultsetNode rrn : session.getTargetKeys()) {
			if (rrn == null) {
				LOGGER.error("null is contained in RoutResultsetNodes, source = "
						+ session.getSource());
				continue;
			}
			final BackendConnection conn = session.getTarget(rrn);
			if (conn != null) {
				//process the XA_END XA_PREPARE Command
				if(conn instanceof MySQLConnection){
					MySQLConnection mysqlCon = (MySQLConnection) conn;
					String xaTxId = null;
					if(session.getXaTXID()!=null){
						xaTxId = session.getXaTXID() +",'"+ mysqlCon.getSchema()+"'";
					}
					if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE)
					{
						//recovery Log
						participantLogEntry[started] = new ParticipantLogEntry(xaTxId,conn.getHost(),0,conn.getSchema(),((MySQLConnection) conn).getXaStatus());
						prepareCount.incrementAndGet();
					}
				}
			}
			started++;
		}
		//xa recovery log
//		if(session.getXaTXID()!=null) {
			CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getXaTXID(), false, participantLogEntry);
			inMemoryRepository.put(session.getXaTXID(), coordinatorLogEntry);
			fileRepository.writeCheckpoint(session.getXaTXID(), inMemoryRepository.getAllCoordinatorLogEntries());
//		}		
	}

	private boolean finished() {
		int val = runningCount.decrementAndGet();
		return (val == 0);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {

	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
//		faileCount.incrementAndGet();
		ErrorPacket errorPacket = new ErrorPacket();
		errorPacket.read(err);
		String msg = new String(errorPacket.message);
		if (LOGGER.isInfoEnabled()){

			LOGGER.info("======================" + "errorResponse from {} msg: {}", conn, new String(errorPacket.message));
		}
		// to do xa prepare failure rollback 
		
		if(conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			if(coorXaStatus == TxState.TX_STARTED_STATE) {
				this.setFail(msg);
				// 1 pc prepare failure to rollback 
				LOGGER.error("XA prepare failure in :xa end;xa prepare; XA id is:" +session.getXaTXID() + " errmsg {},current conn:{}", msg ,conn);
				if(mysqlCon.batchCmdFinished()) {
					if(prepareCount.decrementAndGet() == 0) {
						this.tryErrorFinished(true);
					}
					return ;
				}
			}	
			//replayCommit prepare statue replay commit
			if(coorXaStatus == TxState.TX_PREPARED_STATE) {
			//	if(mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
				String xaTxId = session.getXaTXID();
				if (xaTxId != null) {
					xaTxId += ",'" + mysqlCon.getSchema() + "'";
					String cmd = "XA COMMIT " + xaTxId;
					if (LOGGER.isInfoEnabled()) {
						LOGGER.info("Replay Commit execute the cmd :" + cmd + ",current host:" + mysqlCon.getHost()
								+ ":" + mysqlCon.getPort());
					}
					mysqlCon.execCmd(cmd);
					return;
				}
			}			
		}		
		
		//if some commit and some failure , print error info and return failure info  and then release back connection 
		
		//release connection
		if (this.cmdHandler.releaseConOnErr()) {
			session.releaseConnection(conn);
		} else {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),false);
		}
		
		//
		this.setFail(msg);
		if (this.finished()) {
			this.tryErrorFinished(true);
		}

	}

	
	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		//LOGGER.info("======================" + "okResponse from {} ", conn);

		//process the XA Transatcion 2pc commit
		if(conn instanceof MySQLConnection)
		{   
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus())
			{
				case TxState.TX_STARTED_STATE:
					//if there have many SQL execute wait the okResponse,will come to here one by one
					//should be wait all nodes ready ,then send xa commit to all nodes.
					if (mysqlCon.batchCmdFinished())
					{
						String xaTxId = session.getXaTXID();

						//recovery log
						CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTxId);
						for(int i=0; i<coordinatorLogEntry.participants.length;i++){
							//LOGGER.debug("[In Memory CoordinatorLogEntry]"+coordinatorLogEntry.participants[i]);
							if(coordinatorLogEntry.participants[i].resourceName.equals(conn.getSchema())){
								coordinatorLogEntry.participants[i].txState = TxState.TX_PREPARED_STATE;
							}
						}
						inMemoryRepository.put(xaTxId,coordinatorLogEntry);
//						fileRepository.writeCheckpoint(xaTxId, inMemoryRepository.getAllCoordinatorLogEntries());
						mysqlCon.setXaStatus(TxState.TX_PREPARED_STATE);

						
						//wait all nodes prepare and send all nodes prepare  
						if(prepareCount.decrementAndGet() == 0) {
							fileRepository.writeCheckpoint(xaTxId, inMemoryRepository.getAllCoordinatorLogEntries());
							//判断是否有错误
							if(faileCount.get() > 0) {
								this.tryErrorFinished(true);
								return ;
							}							
							
							coorXaStatus = TxState.TX_PREPARED_STATE; //进入prepare状态 
							for (RouteResultsetNode rrn : session.getTargetKeys()) {														
								if (rrn == null) {
									LOGGER.error("null is contained in RoutResultsetNodes, source = "
											+ session.getSource());
									continue;
								}
								final BackendConnection backConn = session.getTarget(rrn);
								if (conn != null) {
//									conn.setResponseHandler(this);
									//process the XA_END XA_PREPARE Command
									if(conn instanceof MySQLConnection){
										MySQLConnection backMysqlCon = (MySQLConnection) backConn;
										if(session.getXaTXID()!=null){
											xaTxId = session.getXaTXID() +",'"+ backMysqlCon.getSchema()+"'";
										}
										if (backMysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE)
										{
											String cmd = "XA COMMIT " + xaTxId ;
											if (LOGGER.isDebugEnabled()) {
												LOGGER.debug("Start execute the  cmd : "+ cmd+
														" current connection:"+conn.getHost()+":"+conn.getPort());
											}
											backMysqlCon.execCmd(cmd);
										} else {
											LOGGER.info("{} not in PREPARED_STATE", backMysqlCon);
										}
									}
								}
							}
						}				
					}
					return;
				case TxState.TX_PREPARED_STATE:
				{
					//recovery log
					String xaTxId = session.getXaTXID();
					CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTxId);
					for(int i=0; i<coordinatorLogEntry.participants.length;i++){
						if(coordinatorLogEntry.participants[i].resourceName.equals(conn.getSchema())){
							coordinatorLogEntry.participants[i].txState = TxState.TX_COMMITED_STATE;
						}
					}
					inMemoryRepository.put(xaTxId,coordinatorLogEntry);
					fileRepository.writeCheckpoint(xaTxId, inMemoryRepository.getAllCoordinatorLogEntries());

					//XA reset status now
					mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
					break;
				}
				default:
					//	LOGGER.error("Wrong XA status flag!");
			}
		}
		
		if (this.cmdHandler.relaseConOnOK()) {
			if(prepareCount.get() == 0) {
				session.releaseConnection(conn);
			}
		} else {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
		}
		if (this.finished()) {
			if (cmdHandler.isAutoClearSessionCons()) {
				session.clearResources(false);
			}
			/* 1.  事务提交后,xa 事务结束   */
			if(session.getXaTXID()!=null){
				session.setXATXEnabled(false);
			}
			
			/* 2. preAcStates 为true,事务结束后,需要设置为true。preAcStates 为ac上一个状态    */
			if(session.getSource().isPreAcStates()){
				session.getSource().setAutocommit(true);
			}
			cmdHandler.okResponse(session, ok);

		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
//		faileCount.incrementAndGet();
		if (LOGGER.isInfoEnabled()){
			LOGGER.info("======================" + "connectionClose from {} msg: {}", conn, reason);
		}
		
		if(session.getXaTXID() != null) {
			if(conn instanceof MySQLConnection) {
				MySQLConnection mysqlCon = (MySQLConnection) conn;
				if(coorXaStatus == TxState.TX_STARTED_STATE) {
					this.setFail(reason);
					// 1 pc prepare failure to rollback 
					LOGGER.error("XA prepare failure in :xa end;xa prepare; XA id is:" +session.getXaTXID() + " errmsg {},current conn:{}", reason ,conn);
					if(prepareCount.decrementAndGet() == 0) {
						this.tryErrorFinished(true);
					}
					return ;
					
				}	
				//replayCommit prepare statue replay commit
				//todo 重新创建 连接 重新进行提交 conn 是prepare阶段的时候
				if(coorXaStatus == TxState.TX_PREPARED_STATE) {
					String xaTxId = session.getXaTXID();
					if (xaTxId != null) {
						xaTxId += ",'" + mysqlCon.getSchema() + "'";
						String cmd = "XA COMMIT " + xaTxId;
						LOGGER.error("XA Comit failure in :"+cmd+" errmsg {},current conn:{}", reason ,conn);
//						mysqlCon.execCmd(cmd);
						//return;
					}

				}			
			}
		}
		//release connection
		if (this.cmdHandler.releaseConOnErr()) {
			session.releaseConnection(conn);
		} else {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),false);
		}
		
		//设置错误信息
		this.setFail("close back conn reason:"  + reason);
		if (this.finished()) {
			this.tryErrorFinished(true);
//			ErrorPacket errPkg = new ErrorPacket();
//			errPkg.errno = 1;
//			errPkg.message = ("close back conn reason:"  + reason).getBytes();
//			cmdHandler.errorResponse(session, errPkg.writeToBytes() , this.nodeCount,
//					this.faileCount.get());
//			if (cmdHandler.isAutoClearSessionCons()) {
//				session.clearResources(session.getSource().isTxInterrupted());
//			}
		}
	}
	//设置失败
	private void setFail(String err){
		this.error = err;
		faileCount.incrementAndGet();
	}
	protected void tryErrorFinished(boolean allEnd) {
		if (allEnd && !session.closed()) {		
			// xa error auto rollback 
			if(coorXaStatus == TxState.TX_STARTED_STATE ) {
				LOGGER.info("{} found failure in xa ,so to rollback ,reason :{}",this, this.error);
				session.rollback();
				return ;
			} 
			
			// clear session resources,release all
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(this.toString()+"error all end ,clear session resource ");
			}
			//释放连接
			session.clearResources(session.getSource().isTxInterrupted());

			//关闭所有的错误后端连接 清理资源
//			session.closeAndClearResources(error);
			
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId= 1;
			errPkg.errno =  ErrorCode.ER_UNKNOWN_ERROR;
			errPkg.message = (this.error).getBytes();
			session.setAutoCommitStatus();
			cmdHandler.errorResponse(session, errPkg.writeToBytes(), this.nodeCount,
					this.faileCount.get());
			session.getSource().clearTxInterrupt();
			//if (cmdHandler.isAutoClearSessionCons()) {
			//}

		}

	}
}
