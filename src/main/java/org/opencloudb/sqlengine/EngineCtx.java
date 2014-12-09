package org.opencloudb.sqlengine;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.handler.ConfFileHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;

public class EngineCtx {
	public static final Logger LOGGER = Logger.getLogger(ConfFileHandler.class);
	private final BatchSQLJob bachJob;
	private AtomicInteger jobId = new AtomicInteger(0);
	AtomicInteger packetId = new AtomicInteger(0);
	private final NonBlockingSession session;
    private AtomicBoolean finished=new AtomicBoolean(false);
	public EngineCtx(NonBlockingSession session) {
		this.bachJob = new BatchSQLJob();
		this.session = session;
	}

	public byte incPackageId() {
		return (byte) packetId.incrementAndGet();
	}

	public void executeNativeSQLSequnceJob(String[] dataNodes, String sql,
			SQLJobHandler jobHandler) {
		for (String dataNode : dataNodes) {
			SQLJob job = new SQLJob(jobId.incrementAndGet(), sql, dataNode,
					jobHandler, this);
			bachJob.addJob(job, false);

		}
	}

	public void executeNativeSQLParallJob(String[] dataNodes, String sql,
			SQLJobHandler jobHandler) {
		for (String dataNode : dataNodes) {
			SQLJob job = new SQLJob(jobId.incrementAndGet(), sql, dataNode,
					jobHandler, this);
			bachJob.addJob(job, true);

		}
	}

	/**
	 * set no more jobs created
	 */
	public void endJobInput()
	{
		bachJob.setNoMoreJobInput(true);
	}
	public void writeEof() {
		ServerConnection sc = session.getSource();
		EOFPacket eofPckg = new EOFPacket();
		eofPckg.packetId = incPackageId();
		ByteBuffer buf = eofPckg.write(sc.allocate(), sc, false);
		sc.write(buf);
	}

	public NonBlockingSession getSession() {
		return session;
	}

	public void onJobFinished(SQLJob sqlJob) {

		boolean allFinished = bachJob.jobFinished(sqlJob);
		if (allFinished && finished.compareAndSet(false, true)) {
			LOGGER.info("all job finished  for front connection: "
					+ session.getSource());
			writeEof();
		}

	}
}
