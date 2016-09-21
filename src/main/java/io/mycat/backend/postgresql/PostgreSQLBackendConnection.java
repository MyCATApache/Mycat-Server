package io.mycat.backend.postgresql;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.backend.mysql.nio.MySQLConnectionHandler;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.backend.postgresql.packet.Query;
import io.mycat.backend.postgresql.packet.Terminate;
import io.mycat.backend.postgresql.utils.PIOUtils;
import io.mycat.backend.postgresql.utils.PacketUtils;
import io.mycat.backend.postgresql.utils.PgSqlApaterUtils;
import io.mycat.config.Isolations;
import io.mycat.net.BackendAIOConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.TimeUtil;
import io.mycat.util.exception.UnknownTxIsolationException;

/*************************************************************
 * PostgreSQL Native Connection impl
 * 
 * @author Coollf
 *
 */
public class PostgreSQLBackendConnection extends BackendAIOConnection {

	public static enum BackendConnectionState {
		closed, connected, connecting
	}

	private static class StatusSync {
		private final Boolean autocommit;
		private final Integer charsetIndex;
		private final String schema;
		private final AtomicInteger synCmdCount;
		private final Integer txtIsolation;
		private final boolean xaStarted;

		public StatusSync(boolean xaStarted, String schema,
				Integer charsetIndex, Integer txtIsolation, Boolean autocommit,
				int synCount) {
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
			conn.xaStatus = (xaStarted) ? 1 : 0;
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

	private static final Query _COMMIT = new Query("commit");

	private static final Query _ROLLBACK = new Query("rollback");

	private static void getCharsetCommand(StringBuilder sb, int clientCharIndex) {
		sb.append("SET names '")
				.append(CharsetUtil.getCharset(clientCharIndex).toUpperCase())
				.append("';");
	}

	/**
	 * 获取 更改事物级别sql
	 * 
	 * @param
	 * @param txIsolation
	 */
	private static void getTxIsolationCommand(StringBuilder sb, int txIsolation) {
		switch (txIsolation) {
		case Isolations.READ_UNCOMMITTED:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
			return;
		case Isolations.READ_COMMITTED:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;");
			return;
		case Isolations.REPEATED_READ:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
			return;
		case Isolations.SERIALIZABLE:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
			return;
		default:
			throw new UnknownTxIsolationException("txIsolation:" + txIsolation);
		}
	}

	private Object attachment;

	private volatile boolean autocommit;

	private volatile boolean borrowed;

	protected volatile String charset = "utf8";

	protected volatile int charsetIndex;

	/***
	 * 当前事物ID
	 */
	private volatile String currentXaTxId;

	/**
	 * 来自子接口
	 */
	private volatile boolean fromSlaveDB;

	/****
	 * PG是否在事物中
	 */
	private volatile boolean inTransaction = false;

	private AtomicBoolean isQuit = new AtomicBoolean(false);

	private volatile long lastTime;

	/**
	 * 元数据同步
	 */
	private volatile boolean metaDataSyned = true;
	private volatile boolean modifiedSQLExecuted = false;
	private volatile String oldSchema;

	/**
	 * 密码
	 */
	private volatile String password;

	/**
	 * 数据源配置
	 */
	private PostgreSQLDataSource pool;

	/***
	 * 响应handler
	 */
	private volatile ResponseHandler responseHandler;
	/***
	 * 对应数据库空间
	 */
	private volatile String schema;
	// PostgreSQL服务端密码
	private volatile int serverSecretKey;
	private volatile BackendConnectionState state = BackendConnectionState.connecting;
	private volatile StatusSync statusSync;

	private volatile int txIsolation;

	/***
	 * 用户名
	 */
	private volatile String user;

	private volatile int xaStatus;

	public PostgreSQLBackendConnection(NetworkChannel channel,
			boolean fromSlaveDB) {
		super(channel);
		this.fromSlaveDB = fromSlaveDB;
	}

	@Override
	public void commit() {
		ByteBuffer buf = this.allocate();
		_COMMIT.write(buf);
		this.write(buf);
	}

	@Override
	public void execute(RouteResultsetNode rrn, ServerConnection sc,
			boolean autocommit) throws IOException {
		if(LOGGER.isDebugEnabled()){
			LOGGER.debug("{}查询任务。。。。{}", id, rrn.getStatement());
		}
		if (!modifiedSQLExecuted && rrn.isModifySQL()) {
			modifiedSQLExecuted = true;
		}
		String xaTXID = sc.getSession2().getXaTXID();
		synAndDoExecute(xaTXID, rrn, sc.getCharsetIndex(), sc.getTxIsolation(),
				autocommit);
	}

	@Override
	public Object getAttachment() {
		return attachment;
	}

	private void getAutocommitCommand(StringBuilder sb, boolean autoCommit) {
		if (autoCommit) {
			sb.append("SET autocommit=1;");
		} else {
			sb.append("begin transaction;");
		}
	}

	@Override
	public long getLastTime() {
		return lastTime;
	}

	public String getPassword() {
		return password;
	}

	public PostgreSQLDataSource getPool() {
		return pool;
	}

	public ResponseHandler getResponseHandler() {
		return responseHandler;
	}

	@Override
	public String getSchema() {
		return this.schema;
	}

	public int getServerSecretKey() {
		return serverSecretKey;
	}

	public BackendConnectionState getState() {
		return state;
	}

	@Override
	public int getTxIsolation() {
		return txIsolation;
	}

	public String getUser() {
		return user;
	}

	@Override
	public boolean isAutocommit() {
		return autocommit;
	}

	@Override
	public boolean isBorrowed() {
		return borrowed;
	}

	@Override
	public boolean isClosedOrQuit() {
		return isClosed() || isQuit.get();
	}

	@Override
	public boolean isFromSlaveDB() {
		return fromSlaveDB;
	}

	public boolean isInTransaction() {
		return inTransaction;
	}

	@Override
	public boolean isModifiedSQLExecuted() {
		return modifiedSQLExecuted;
	}

	@Override
	public void onConnectFailed(Throwable t) {
		if (handler instanceof MySQLConnectionHandler) {

		}
	}

	@Override
	public void onConnectfinish() {
		LOGGER.debug("连接后台真正完成");
		try {
			SocketChannel chan = (SocketChannel) this.channel;
			ByteBuffer buf = PacketUtils.makeStartUpPacket(user, schema);
			buf.flip();
			chan.write(buf);
		} catch (Exception e) {
			LOGGER.error("Connected PostgreSQL Send StartUpPacket ERROR", e);
			throw new RuntimeException(e);
		}
	}
	
	protected final int getPacketLength(ByteBuffer buffer, int offset) {
		//Pg 协议获取包长度的方法和mysql 不一样
		return PIOUtils.redInteger4(buffer, offset+1)+1;		
	}
	


	
	
	/**********
	 * 此查询用于心跳检查和获取连接后的健康检查
	 */
	@Override
	public void query(String query) throws UnsupportedEncodingException {
		RouteResultsetNode rrn = new RouteResultsetNode("default",
				ServerParse.SELECT, query);
		synAndDoExecute(null, rrn, this.charsetIndex, this.txIsolation, true);
	}

	@Override
	public void quit() {
		if (isQuit.compareAndSet(false, true) && !isClosed()) {
			if (state == BackendConnectionState.connected) {// 断开 与PostgreSQL连接
				Terminate terminate = new Terminate();
				ByteBuffer buf = this.allocate();
				terminate.write(buf);
				write(buf);
			} else {
				close("normal");
			}
		}
	}

	/*******
	 * 记录sql执行信息
	 */
	@Override
	public void recordSql(String host, String schema, String statement) {
		LOGGER.debug(String.format(
				"executed sql: host=%s,schema=%s,statement=%s", host, schema,
				statement));
	}

	@Override
	public void release() {
		if (!metaDataSyned) {/*
							 * indicate connection not normalfinished ,and we
							 * can't know it's syn status ,so close it
							 */

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
	public void rollback() {
		ByteBuffer buf = this.allocate();
		_ROLLBACK.write(buf);
		this.write(buf);
	}

	@Override
	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	@Override
	public void setBorrowed(boolean borrowed) {
		this.borrowed = borrowed;
	}

	public void setInTransaction(boolean inTransaction) {
		this.inTransaction = inTransaction;
	}

	@Override
	public void setLastTime(long currentTimeMillis) {
		this.lastTime = currentTimeMillis;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setPool(PostgreSQLDataSource pool) {
		this.pool = pool;
	}

	@Override
	public boolean setResponseHandler(ResponseHandler commandHandler) {
		this.responseHandler = commandHandler;
		return true;
	}

	@Override
	public void setSchema(String newSchema) {
		String curSchema = schema;
		if (curSchema == null) {
			this.schema = newSchema;
			this.oldSchema = newSchema;
		} else {
			this.oldSchema = curSchema;
			this.schema = newSchema;
		}
	}

	public void setServerSecretKey(int serverSecretKey) {
		this.serverSecretKey = serverSecretKey;
	}

	public void setState(BackendConnectionState state) {
		this.state = state;
	}

	public void setUser(String user) {
		this.user = user;
	}

	private void synAndDoExecute(String xaTxID, RouteResultsetNode rrn,
			int clientCharSetIndex, int clientTxIsoLation,
			boolean clientAutoCommit) {
		String xaCmd = null;

		boolean conAutoComit = this.autocommit;
		String conSchema = this.schema;
		// never executed modify sql,so auto commit
		boolean expectAutocommit = !modifiedSQLExecuted || isFromSlaveDB()
				|| clientAutoCommit;
		if (!expectAutocommit && xaTxID != null && xaStatus == 0) {
			clientTxIsoLation = Isolations.SERIALIZABLE;
			xaCmd = "XA START " + xaTxID + ';';
			currentXaTxId = xaTxID;
		}
		int schemaSyn = conSchema.equals(oldSchema) ? 0 : 1;
		int charsetSyn = (this.charsetIndex == clientCharSetIndex) ? 0 : 1;
		int txIsoLationSyn = (txIsolation == clientTxIsoLation) ? 0 : 1;
		int autoCommitSyn = (conAutoComit == expectAutocommit) ? 0 : 1;
		int synCount = schemaSyn + charsetSyn + txIsoLationSyn + autoCommitSyn;

		if (synCount == 0) {
			String sql = rrn.getStatement();
			Query query = new Query(PgSqlApaterUtils.apater(sql));
			ByteBuffer buf = this.allocate();// XXX 此处处理问题
			query.write(buf);
			this.write(buf);
			return;
		}

		// TODO COOLLF 此处大锅待实现. 相关 事物, 切换 库,自动提交等功能实现
		StringBuilder sb = new StringBuilder();
		if (charsetSyn == 1) {
			getCharsetCommand(sb, clientCharSetIndex);
		}
		if (txIsoLationSyn == 1) {
			getTxIsolationCommand(sb, clientTxIsoLation);
		}
		if (autoCommitSyn == 1) {
			getAutocommitCommand(sb, expectAutocommit);
		}
		if (xaCmd != null) {
			sb.append(xaCmd);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("con need syn ,total syn cmd " + synCount
					+ " commands " + sb.toString() + "schema change:"
					+ ("" != null) + " con:" + this);
		}

		metaDataSyned = false;
		statusSync = new StatusSync(xaCmd != null, conSchema,
				clientCharSetIndex, clientTxIsoLation, expectAutocommit,
				synCount);
		String sql = sb.append(PgSqlApaterUtils.apater(rrn.getStatement()))
				.toString();
		LOGGER.debug("con={}, SQL:{}", this, sql);
		Query query = new Query(sql);
		ByteBuffer buf = allocate();// 申请ByetBuffer
		query.write(buf);
		this.write(buf);
		metaDataSyned = true;
	}

	

	public void close(String reason) {
		if (!isClosed.get()) {
			isQuit.set(true);
			super.close(reason);
			pool.connectionClosed(this);
			if (this.responseHandler != null) {
				this.responseHandler.connectionClose(this, reason);
				responseHandler = null;
			}
		}
	}

	@Override
	public boolean syncAndExcute() {
		StatusSync sync = this.statusSync;
		if (sync != null) {
			boolean executed = sync.synAndExecuted(this);
			if (executed) {
				statusSync = null;
			}
			return executed;
		}
		return true;
	}

	
	@Override
	public String toString() {
		return "PostgreSQLBackendConnection [id=" + id + ", host=" + host + ", port="
				+ port + ", localPort=" + localPort + "]";
	}
}
