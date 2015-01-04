package org.opencloudb.mysql.nio.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.sqlcmd.SQLCtrlCommand;

public class MultiNodeCoordinator implements ResponseHandler {
	private static final Logger LOGGER = Logger
			.getLogger(MultiNodeCoordinator.class);
	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final AtomicInteger faileCount = new AtomicInteger(0);
	private volatile int nodeCount;
	private final NonBlockingSession session;
	private SQLCtrlCommand cmdHandler;
	private final AtomicBoolean failed = new AtomicBoolean(false);

	public MultiNodeCoordinator(NonBlockingSession session) {
		this.session = session;
	}

	public void executeBatchNodeCmd(SQLCtrlCommand cmdHandler) {
		this.cmdHandler = cmdHandler;
		final int initCount = session.getTargetCount();
		runningCount.set(initCount);
		nodeCount = initCount;
		failed.set(false);
		faileCount.set(0);
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
				cmdHandler.sendCommand(session, conn);
				++started;
			}
		}

		if (started < nodeCount) {
			runningCount.set(started);
			LOGGER.warn("some connection failed to execut "
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

		if (this.cmdHandler.releaseConOnErr()) {
			session.releaseConnection(conn);
		} else {
			
			
			
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
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
