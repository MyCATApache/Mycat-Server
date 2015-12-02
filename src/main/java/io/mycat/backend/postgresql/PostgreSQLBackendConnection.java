package io.mycat.backend.postgresql;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.backend.postgresql.packet.Query;
import io.mycat.backend.postgresql.packet.Terminate;
import io.mycat.backend.postgresql.utils.PgSqlApaterUtils;
import io.mycat.net.Connection;
import io.mycat.net.NetSystem;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.Isolations;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.executors.ResponseHandler;
import io.mycat.server.packet.util.CharsetUtil;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/*************************************************************
 * PostgreSQL Native Connection impl
 * 
 * @author Coollf
 *
 */
public class PostgreSQLBackendConnection extends Connection implements BackendConnection {
	private static final Query _READ_UNCOMMITTED = new Query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
	private static final Query _READ_COMMITTED = new Query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
	private static final Query _REPEATED_READ = new Query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	private static final Query _SERIALIZABLE = new Query("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE");
	private static final Query _AUTOCOMMIT_ON = new Query("SET autocommit=1");
	private static final Query _AUTOCOMMIT_OFF = new Query("SET autocommit=0");
	private static final Query _COMMIT = new Query("rollback");
	private static final Query _ROLLBACK = new Query("commit");
	
	
	/**
	 * 来自子接口
	 */
	private boolean fromSlaveDB;

	/***
	 * 用户名
	 */
	private String user;

	/**
	 * 密码
	 */
	private String password;

	/***
	 * 对应数据库空间
	 */
	private String schema;

	/**
	 * 数据源配置
	 */
	private PostgreSQLDataSource pool;
	private Object attachment;
	protected volatile String charset = "utf8";
	private volatile boolean autocommit;
	private volatile long currentTimeMillis;
	
	
	/****
	 * PG是否在事物中
	 */
	private volatile boolean inTransaction = false;

	/***
	 * 响应handler
	 */
	private volatile ResponseHandler responseHandler;
	private boolean borrowed;
	private volatile int txIsolation;
	private volatile boolean modifiedSQLExecuted = false;
	private long lastTime;
	private AtomicBoolean isQuit;

	// PostgreSQL服务端密码
	private int serverSecretKey;

	protected volatile int charsetIndex;

	private int xaStatus;

	private Object oldSchema;

	// 已经认证通过
	private boolean isAuthenticated;

	private volatile boolean metaDataSyned = true;

	private volatile StatusSync statusSync;

	public PostgreSQLBackendConnection(SocketChannel channel, boolean fromSlaveDB) {
		super(channel);
		this.fromSlaveDB = fromSlaveDB;
		this.lastTime = TimeUtil.currentTimeMillis();
		this.isQuit = new AtomicBoolean(false);
		this.autocommit = true;
	}

	@Override
	public boolean isFromSlaveDB() {
		return fromSlaveDB;
	}

	@Override
	public String getSchema() {
		return schema;
	}

	@Override
	public void setSchema(String newSchema) {
		this.schema = newSchema;
	}

	@Override
	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	@Override
	public void setLastTime(long currentTimeMillis) {
		this.currentTimeMillis = currentTimeMillis;
	}

	@Override
	public void setResponseHandler(ResponseHandler queryHandler) {
		this.responseHandler = queryHandler;
	}

	@Override
	public Object getAttachment() {
		return attachment;
	}

	@Override
	public boolean isBorrowed() {
		return borrowed;
	}

	@Override
	public void setBorrowed(boolean borrowed) {
		this.lastTime = TimeUtil.currentTimeMillis();
		this.borrowed = borrowed;
	}

	@Override
	public int getTxIsolation() {
		return txIsolation;
	}

	@Override
	public boolean isAutocommit() {
		return autocommit;
	}

	@Override
	public String getCharset() {
		return charset;
	}

	@Override
	public PhysicalDatasource getPool() {
		return pool;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user
	 *            the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @param fromSlaveDB
	 *            the fromSlaveDB to set
	 */
	public void setFromSlaveDB(boolean fromSlaveDB) {
		this.fromSlaveDB = fromSlaveDB;
	}

	/**
	 * @param pool
	 *            the pool to set
	 */
	public void setPool(PostgreSQLDataSource pool) {
		this.pool = pool;
	}

	@Override
	public boolean isModifiedSQLExecuted() {
		return modifiedSQLExecuted;
	}

	@Override
	public long getLastTime() {
		return lastTime;
	}

	@Override
	public boolean isClosedOrQuit() {
		return isClosed() || isQuit.get();
	}

	@Override
	public void quit() {
		if (isQuit.compareAndSet(false, true) && !isClosed()) {
			if (isAuthenticated) {// 断开 与PostgreSQL连接
				Terminate terminate = new Terminate();
				ByteBuffer buf = NetSystem.getInstance().getBufferPool().allocate();
				terminate.write(buf);
				write(buf);
			} else {
				close("normal");
			}
		}
	}

	@Override
	public void release() {
		if (metaDataSyned == false) {// indicate connection not normalfinished
										// ,and
										// we can't know it's syn status ,so
										// close
										// it
			LOGGER.warn("can't sure connection syn result,so close it " + this);
			this.responseHandler = null;
			this.close("syn status unkown ");
			return;
		}
		metaDataSyned = true;
		attachment = null;
		statusSync = null;
		modifiedSQLExecuted = false;
		setResponseHandler(null);
		pool.releaseChannel(this);
	}

	@Override
	public void query(String query) throws UnsupportedEncodingException {
		RouteResultsetNode rrn = new RouteResultsetNode("default", ServerParse.SELECT, query);
		synAndDoExecute(null, rrn, this.charsetIndex, this.txIsolation, true);
	}

	@Override
	public void execute(RouteResultsetNode rrn, MySQLFrontConnection sc, boolean autocommit) throws IOException {
		if (!modifiedSQLExecuted && rrn.isModifySQL()) {
			modifiedSQLExecuted = true;
		}
		String xaTXID = sc.getSession2().getXaTXID();
		synAndDoExecute(xaTXID, rrn, sc, sc.getCharsetIndex(), sc.getTxIsolation(), autocommit);

	}

	private void synAndDoExecute(String xaTXID, RouteResultsetNode rrn, MySQLFrontConnection sc, int charsetIndex,
			int txIsolation2, boolean clientAutoCommit) {
		boolean conAutoComit = this.autocommit;
		String conSchema = this.schema;
		String sql = rrn.getStatement();
		System.err.println("SQL->:" + sql);
		int sqlType = rrn.getSqlType();
		if (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW) {
			Query query = new Query(PgSqlApaterUtils.apater(sql));
			ByteBuffer buf = NetSystem.getInstance().getBufferPool().allocate();
			query.write(buf);
			this.write(buf);
		} else {// DDL
			// 执行命令语句
			Query query = new Query(PgSqlApaterUtils.apater(sql));
			ByteBuffer buf = NetSystem.getInstance().getBufferPool().allocate();
			query.write(buf);
			this.write(buf);
		}
	}

	private void synAndDoExecute(String xaTxID, RouteResultsetNode rrn, int clientCharSetIndex, int clientTxIsoLation,
			boolean clientAutoCommit) {
		String xaCmd = null;

		boolean conAutoComit = this.autocommit;
		String conSchema = this.schema;
		// never executed modify sql,so auto commit
		boolean expectAutocommit = !modifiedSQLExecuted || isFromSlaveDB() || clientAutoCommit;
		if (expectAutocommit == false && xaTxID != null && xaStatus == 0) {
			clientTxIsoLation = Isolations.SERIALIZABLE;
			xaCmd = "XA START " + xaTxID + ';';

		}
		int schemaSyn = conSchema.equals(oldSchema) ? 0 : 1;
		int charsetSyn = (this.charsetIndex == clientCharSetIndex) ? 0 : 1;
		int txIsoLationSyn = (txIsolation == clientTxIsoLation) ? 0 : 1;
		int autoCommitSyn = (conAutoComit == expectAutocommit) ? 0 : 1;
		int synCount = schemaSyn + charsetSyn + txIsoLationSyn + autoCommitSyn;
		// TODO COOLLF 此处大锅待实现. 相关 事物, 切换 库,自动提交等功能实现.

		metaDataSyned = false;
		statusSync = new StatusSync(xaCmd != null, conSchema, clientCharSetIndex, clientTxIsoLation, expectAutocommit,
				synCount);
		Query query = new Query(rrn.getStatement());
		ByteBuffer buf = NetSystem.getInstance().getBufferPool().allocate();
		query.write(buf);
		this.write(buf);
		metaDataSyned = true;
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean syncAndExcute() {
		return true;
	}
	

	@Override
	public void rollback() {
	
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onReadData(int got) throws IOException {
		LOGGER.debug("能读取 {} 长度的数据包", got);
		this.handler.handle(this, getReadBuffer(), 0, got);
		if(getReadBuffer()!=null){
			getReadBuffer().clear();// 使用完成后清理
		}else{
			System.err.println("getReadBuffer()为空");
		}
	}

	public void setServerSecretKey(int serverSecretKey) {
		this.serverSecretKey = serverSecretKey;
	}

	/**
	 * @return the serverSecretKey
	 */
	public int getServerSecretKey() {
		return serverSecretKey;
	}

	/**
	 * @return the responseHandler
	 */
	public ResponseHandler getResponseHandler() {
		return responseHandler;
	}

	private static class StatusSync {
		private final String schema;
		private final Integer charsetIndex;
		private final Integer txtIsolation;
		private final Boolean autocommit;
		private final AtomicInteger synCmdCount;
		private final boolean xaStarted;

		public StatusSync(boolean xaStarted, String schema, Integer charsetIndex, Integer txtIsolation,
				Boolean autocommit, int synCount) {
			super();
			this.xaStarted = xaStarted;
			this.schema = schema;
			this.charsetIndex = charsetIndex;
			this.txtIsolation = txtIsolation;
			this.autocommit = autocommit;
			this.synCmdCount = new AtomicInteger(synCount);
		}

		public boolean synAndExecuted(PostgreSQLBackendConnection conn) {
			int remains = synCmdCount.decrementAndGet();
			if (remains == 0) {// syn command finished
				this.updateConnectionInfo(conn);
				conn.metaDataSyned = true;
				return false;
			} else if (remains < 0) {
				return true;
			}
			return false;
		}

		private void updateConnectionInfo(PostgreSQLBackendConnection conn)

		{
			conn.xaStatus = (xaStarted == true) ? 1 : 0;
			if (schema != null) {
				conn.schema = schema;
				conn.oldSchema = conn.schema;
			}
			if (charsetIndex != null) {
				conn.setCharset(CharsetUtil.getCharset(charsetIndex));
			}
			if (txtIsolation != null) {
				conn.txIsolation = txtIsolation;
			}
			if (autocommit != null) {
				conn.autocommit = autocommit;
			}
		}

	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * @return the inTransaction
	 */
	public boolean isInTransaction() {
		return inTransaction;
	}

	/**
	 * @param inTransaction the inTransaction to set
	 */
	public void setInTransaction(boolean inTransaction) {
		this.inTransaction = inTransaction;
	}

}
