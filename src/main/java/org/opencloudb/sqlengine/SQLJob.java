package org.opencloudb.sqlengine;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.ConnectionMeta;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;

public class SQLJob implements ResponseHandler, Runnable {
	private final String sql;
	private final String dataNode;
	private final SQLJobHandler jobHandler;
	private final EngineCtx ctx;
	private final int id;

	public SQLJob(int id, String sql, String dataNode,
			SQLJobHandler jobHandler, EngineCtx ctx) {
		super();
		this.id = id;
		this.sql = sql;
		this.dataNode = dataNode;
		this.jobHandler = jobHandler;
		this.ctx = ctx;
	}

	public void run() {
		RouteResultsetNode node = new RouteResultsetNode(dataNode,
				ServerParse.SELECT, sql);
		// create new connection
		ServerConnection sc = ctx.getSession().getSource();
		MycatConfig conf = MycatServer.getInstance().getConfig();
		PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
		ConnectionMeta conMeta = new ConnectionMeta(dn.getDatabase(),
				sc.getCharset(), sc.getCharsetIndex(), true);
		try {
			dn.getConnection(conMeta, node, this, node);
		} catch (Exception e) {
			EngineCtx.LOGGER.info("can't get connection for sql ,error:" + e);
			doFinished(true);
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		if (EngineCtx.LOGGER.isDebugEnabled()) {
			EngineCtx.LOGGER.debug("con query sql:" + sql + " to con:" + conn);
		}
		conn.setResponseHandler(this);
		try {
			conn.query(sql);
		} catch (UnsupportedEncodingException e) {
			doFinished(true);
		}

	}

	private void doFinished(boolean failed) {
		jobHandler.finished(dataNode, failed);
		ctx.onJobFinished(this);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		EngineCtx.LOGGER.info("can't get connection for sql :" + sql);
		doFinished(true);

	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		ErrorPacket errPg = new ErrorPacket();
		errPg.read(err);
		EngineCtx.LOGGER.info("error response " + new String(errPg.message)
				+ " from of sql :" + sql + " at con:" + conn);
		conn.release();
		doFinished(true);

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		// not called for query sql

	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		jobHandler.onHeader(dataNode, header, fields);

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		boolean finsihed = jobHandler.onRowData(dataNode, row);
		if (finsihed) {
			conn.close("not needed by user proc");
			doFinished(false);
		}

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		conn.release();
		doFinished(false);
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		doFinished(true);
	}

	public int getId() {
		return id;
	}

	@Override
	public String toString() {
		return "SQLJob [ id=" + id + ",dataNode=" + dataNode
				+ ",sql=" + sql + ",  jobHandler=" + jobHandler + "]";
	}

}
