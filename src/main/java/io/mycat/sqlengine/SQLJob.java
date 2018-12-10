package io.mycat.sqlengine;

import java.util.List;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.MycatConfig;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;

/**
 * asyn execute in EngineCtx or standalone (EngineCtx=null)
 * 
 * @author wuzhih
 * 
 */
public class SQLJob implements ResponseHandler, Runnable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);
	
	private final String sql;
	private final String dataNodeOrDatabase;
	private BackendConnection connection;
	private final SQLJobHandler jobHandler;
	private final EngineCtx ctx;
	private final PhysicalDatasource ds;
	private final int id;
	private volatile boolean finished;

	public SQLJob(int id, String sql, String dataNode,
			SQLJobHandler jobHandler, EngineCtx ctx) {
		super();
		this.id = id;
		this.sql = sql;
		this.dataNodeOrDatabase = dataNode;
		this.jobHandler = jobHandler;
		this.ctx = ctx;
		this.ds = null;
	}

	public SQLJob(String sql, String databaseName, SQLJobHandler jobHandler,
			PhysicalDatasource ds) {
		super();
		this.id = 0;
		this.sql = sql;
		this.dataNodeOrDatabase = databaseName;
		this.jobHandler = jobHandler;
		this.ctx = null;
		this.ds = ds;

	}

	public void run() {
		try {
			if (ds == null) {
				RouteResultsetNode node = new RouteResultsetNode(
						dataNodeOrDatabase, ServerParse.SELECT, sql);
				// create new connection
				MycatConfig conf = MycatServer.getInstance().getConfig();
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), true, node, this, node);
			} else {
				ds.getConnection(dataNodeOrDatabase, true, this, null);
			}
		} catch (Exception e) {
			LOGGER.info("can't get connection for sql ,error:" ,e);
			doFinished(true,e.getMessage());
		}
	}

	public void teminate(String reason) {
		LOGGER.info("terminate this job reason:" + reason + " con:"
				+ connection + " sql " + this.sql);
		if (connection != null) {
			connection.close(reason);
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("con query sql:" + sql + " to con:" + conn);
		}
		conn.setResponseHandler(this);
		try {
			if(ctx != null) {
				ServerConnection sc = ctx.getSession().getSource();
				//conn.setCharsetIndex(sc.getCharsetIndex());				
				conn.query(sql ,sc.getCharsetIndex());
			}else {
				conn.query(sql );
			}
			
			connection = conn;
		} catch (Exception e) {// (UnsupportedEncodingException e) {
			doFinished(true,e.getMessage());
		}

	}

	public boolean isFinished() {
		return finished;
	}

	private void doFinished(boolean failed,String errorMsg) {
		finished = true;
		jobHandler.finished(dataNodeOrDatabase, failed,errorMsg );
		if (ctx != null) {
			if(failed){
				ctx.setHasError(true);
			}
			ctx.onJobFinished(this);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		LOGGER.info("can't get connection for sql :" + sql);
		doFinished(true,e.getMessage());
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		ErrorPacket errPg = new ErrorPacket();
		errPg.read(err);
		
		String errMsg = "error response errno:" + errPg.errno + ", " + new String(errPg.message)
				+ " from of sql :" + sql + " at con:" + conn;
		
		// @see https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
		// ER_SPECIFIC_ACCESS_DENIED_ERROR
		if ( errPg.errno == 1227  ) {
			LOGGER.warn( errMsg );	
			
		}  else {
			LOGGER.info( errMsg );
		}
		
		
		
		doFinished(true,errMsg);
		conn.release();
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
//		conn.syncAndExcute();
		//modify by zwy  这边 涉及到use database的返回，不能直接释放连接 需要继续处理包
		boolean executeResponse = conn.syncAndExcute();		
		if(executeResponse){
			doFinished(false,null);
			conn.release();
		} else {
			LOGGER.debug("syn response {}" ,conn);
		}
		
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		jobHandler.onHeader(dataNodeOrDatabase, header, fields);

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		boolean finsihed = jobHandler.onRowData(dataNodeOrDatabase, row);
		if (finsihed) {
			doFinished(false,null);
			conn.close("not needed by user proc");
		}

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		doFinished(false,null);
		conn.release();
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		doFinished(true,reason);
	}

	public int getId() {
		return id;
	}

	@Override
	public String toString() {
		return "SQLJob [ id=" + id + ",dataNodeOrDatabase="
				+ dataNodeOrDatabase + ",sql=" + sql + ",  jobHandler="
				+ jobHandler + "]";
	}

}
