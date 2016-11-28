package io.mycat.backend.mysql.nio.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.esotericsoftware.minlog.Log;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.ParticipantLogEntry;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.backend.mysql.xa.recovery.impl.FileSystemRepository;
import io.mycat.backend.mysql.xa.recovery.impl.InMemoryRepository;
import io.mycat.net.BackendAIOConnection;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.sqlcmd.SQLCtrlCommand;

public class MultiNodeCoordinator implements ResponseHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MultiNodeCoordinator.class);
	public static final Repository fileRepository = new FileSystemRepository();
	public static final Repository inMemoryRepository = new InMemoryRepository();
	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final AtomicInteger faileCount = new AtomicInteger(0);
	private volatile int nodeCount;
	private final NonBlockingSession session;
	private SQLCtrlCommand cmdHandler;
	private final AtomicBoolean failed = new AtomicBoolean(false);

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
		//recovery nodes log
		ParticipantLogEntry[] participantLogEntry = new ParticipantLogEntry[initCount];
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
				MySQLConnection mysqlCon = (MySQLConnection) conn;
				String xaTxId = session.getXaTXID();
				if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE)
				{
					//recovery Log
					participantLogEntry[started] = new ParticipantLogEntry(xaTxId,conn.getHost(),0,conn.getSchema(),((MySQLConnection) conn).getXaStatus());
					String[] cmds = new String[]{"XA END " + xaTxId,
							"XA PREPARE " + xaTxId};
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Start execute the batch cmd : "+ cmds[0] + ";" + cmds[1]+","+
								"current connection:"+conn.getHost()+":"+conn.getPort());
					}
					mysqlCon.execBatchCmd(cmds);
				} else
				{
					//recovery Log
					participantLogEntry[started] = new ParticipantLogEntry(xaTxId,conn.getHost(),0,conn.getSchema(),((MySQLConnection) conn).getXaStatus());
					cmdHandler.sendCommand(session, conn);
				}
				++started;
			}
		}

		//xa recovery log
		if(session.getXaTXID()!=null) {
			CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getXaTXID(), false, participantLogEntry);
			inMemoryRepository.put(session.getXaTXID(), coordinatorLogEntry);
			fileRepository.writeCheckpoint(inMemoryRepository.getAllCoordinatorLogEntries());
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
		faileCount.incrementAndGet();

		//replayCommit
		if(conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			String xaTxId = session.getXaTXID();
			if (xaTxId != null) {
				String cmd = "XA COMMIT " + xaTxId;
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Replay Commit execute the cmd :" + cmd + ",current host:" +
							mysqlCon.getHost() + ":" + mysqlCon.getPort());
				}
				mysqlCon.execCmd(cmd);
			}
		}

		//release connection
		if (this.cmdHandler.releaseConOnErr()) {
			session.releaseConnection(conn);
		} else {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),false);
		}
		if (this.finished()) {
			cmdHandler.errorResponse(session, err, this.nodeCount,
					this.faileCount.get());
			if (cmdHandler.isAutoClearSessionCons()) {
				session.clearResources(session.getSource().isTxInterrupted());
			}
		}

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
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
						String cmd = "XA COMMIT " + xaTxId;
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Start execute the cmd :"+cmd+",current host:"+
									mysqlCon.getHost()+":"+mysqlCon.getPort());
						}
						//recovery log
						CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTxId);
						for(int i=0; i<coordinatorLogEntry.participants.length;i++){
							LOGGER.debug("[In Memory CoordinatorLogEntry]"+coordinatorLogEntry.participants[i]);
							if(coordinatorLogEntry.participants[i].resourceName.equals(conn.getSchema())){
								coordinatorLogEntry.participants[i].txState = TxState.TX_PREPARED_STATE;
							}
						}
						inMemoryRepository.put(session.getXaTXID(),coordinatorLogEntry);
						fileRepository.writeCheckpoint(inMemoryRepository.getAllCoordinatorLogEntries());

						//send commit
						mysqlCon.setXaStatus(TxState.TX_PREPARED_STATE);
						mysqlCon.execCmd(cmd);
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
					inMemoryRepository.put(session.getXaTXID(),coordinatorLogEntry);
					fileRepository.writeCheckpoint(inMemoryRepository.getAllCoordinatorLogEntries());

					//XA reset status now
					mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
					break;
				}
				default:
					//	LOGGER.error("Wrong XA status flag!");
			}
		}

		if (this.cmdHandler.relaseConOnOK()) {
			session.releaseConnection(conn);
		} else {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
		}
		if (this.finished()) {
			cmdHandler.okResponse(session, ok);
			if (cmdHandler.isAutoClearSessionCons()) {
				session.clearResources(false);
			}

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

	}

}
