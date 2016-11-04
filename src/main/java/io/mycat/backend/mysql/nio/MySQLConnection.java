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
package io.mycat.backend.mysql.nio;

import io.mycat.backend.mysql.xa.TxState;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.backend.mysql.SecurityUtil;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.Capabilities;
import io.mycat.config.Isolations;
import io.mycat.net.BackendAIOConnection;
import io.mycat.net.mysql.*;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.TimeUtil;
import io.mycat.util.exception.UnknownTxIsolationException;

import java.io.UnsupportedEncodingException;
import java.nio.channels.NetworkChannel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mycat
 */
public class MySQLConnection extends BackendAIOConnection {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MySQLConnection.class);
	private static final long CLIENT_FLAGS = initClientFlags();
	private volatile long lastTime; 
	private volatile String schema = null;
	private volatile String oldSchema;
	private volatile boolean borrowed = false;
	private volatile boolean modifiedSQLExecuted = false;
	private volatile int batchCmdCount = 0;

	private static long initClientFlags() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		boolean usingCompress=MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
		if(usingCompress)
		{
			 flag |= Capabilities.CLIENT_COMPRESS;
		}
		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= Capabilities.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		// client extension
		flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
		flag |= Capabilities.CLIENT_MULTI_RESULTS;
		return flag;
	}

	private static final CommandPacket _READ_UNCOMMITTED = new CommandPacket();
	private static final CommandPacket _READ_COMMITTED = new CommandPacket();
	private static final CommandPacket _REPEATED_READ = new CommandPacket();
	private static final CommandPacket _SERIALIZABLE = new CommandPacket();
	private static final CommandPacket _AUTOCOMMIT_ON = new CommandPacket();
	private static final CommandPacket _AUTOCOMMIT_OFF = new CommandPacket();
	private static final CommandPacket _COMMIT = new CommandPacket();
	private static final CommandPacket _ROLLBACK = new CommandPacket();
	static {
		_READ_UNCOMMITTED.packetId = 0;
		_READ_UNCOMMITTED.command = MySQLPacket.COM_QUERY;
		_READ_UNCOMMITTED.arg = "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
				.getBytes();
		_READ_COMMITTED.packetId = 0;
		_READ_COMMITTED.command = MySQLPacket.COM_QUERY;
		_READ_COMMITTED.arg = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
				.getBytes();
		_REPEATED_READ.packetId = 0;
		_REPEATED_READ.command = MySQLPacket.COM_QUERY;
		_REPEATED_READ.arg = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
				.getBytes();
		_SERIALIZABLE.packetId = 0;
		_SERIALIZABLE.command = MySQLPacket.COM_QUERY;
		_SERIALIZABLE.arg = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"
				.getBytes();
		_AUTOCOMMIT_ON.packetId = 0;
		_AUTOCOMMIT_ON.command = MySQLPacket.COM_QUERY;
		_AUTOCOMMIT_ON.arg = "SET autocommit=1".getBytes();
		_AUTOCOMMIT_OFF.packetId = 0;
		_AUTOCOMMIT_OFF.command = MySQLPacket.COM_QUERY;
		_AUTOCOMMIT_OFF.arg = "SET autocommit=0".getBytes();
		_COMMIT.packetId = 0;
		_COMMIT.command = MySQLPacket.COM_QUERY;
		_COMMIT.arg = "commit".getBytes();
		_ROLLBACK.packetId = 0;
		_ROLLBACK.command = MySQLPacket.COM_QUERY;
		_ROLLBACK.arg = "rollback".getBytes();
	}

	private MySQLDataSource pool;
	private boolean fromSlaveDB;
	private long threadId;
	private HandshakePacket handshake;
	private volatile int txIsolation;
	private volatile boolean autocommit;
	private long clientFlags;
	private boolean isAuthenticated;
	private String user;
	private String password;
	private Object attachment;
	private ResponseHandler respHandler;

	private final AtomicBoolean isQuit;
	private volatile StatusSync statusSync;
	private volatile boolean metaDataSyned = true;
	private volatile int xaStatus = 0;

	public MySQLConnection(NetworkChannel channel, boolean fromSlaveDB) {
		super(channel);
		this.clientFlags = CLIENT_FLAGS;
		this.lastTime = TimeUtil.currentTimeMillis();
		this.isQuit = new AtomicBoolean(false);
		this.autocommit = true;
		this.fromSlaveDB = fromSlaveDB;
		// 设为默认值，免得每个初始化好的连接都要去同步一下
		this.txIsolation = MycatServer.getInstance().getConfig().getSystem().getTxIsolation();
	}

	public int getXaStatus() {
		return xaStatus;
	}

	public void setXaStatus(int xaStatus) {
		this.xaStatus = xaStatus;
	}

	public void onConnectFailed(Throwable t) {
		if (handler instanceof MySQLConnectionHandler) {
			MySQLConnectionHandler theHandler = (MySQLConnectionHandler) handler;
			theHandler.connectionError(t);
		} else {
			((MySQLConnectionAuthenticator) handler).connectionError(this, t);
		}
	}

	public String getSchema() {
		return this.schema;
	}

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

	public MySQLDataSource getPool() {
		return pool;
	}

	public void setPool(MySQLDataSource pool) {
		this.pool = pool;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public HandshakePacket getHandshake() {
		return handshake;
	}

	public void setHandshake(HandshakePacket handshake) {
		this.handshake = handshake;
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public boolean isAuthenticated() {
		return isAuthenticated;
	}

	public void setAuthenticated(boolean isAuthenticated) {
		this.isAuthenticated = isAuthenticated;
	}

	public String getPassword() {
		return password;
	}

	public void authenticate() {
		AuthPacket packet = new AuthPacket();
		packet.packetId = 1;
		packet.clientFlags = clientFlags;
		packet.maxPacketSize = maxPacketSize;
		packet.charsetIndex = this.charsetIndex;
		packet.user = user;
		try {
			packet.password = passwd(password, handshake);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
		packet.database = schema;
		packet.write(this);
	}

	public boolean isAutocommit() {
		return autocommit;
	}

	public Object getAttachment() {
		return attachment;
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public boolean isClosedOrQuit() {
		return isClosed() || isQuit.get();
	}

	protected void sendQueryCmd(String query) {
		CommandPacket packet = new CommandPacket();
		packet.packetId = 0;
		packet.command = MySQLPacket.COM_QUERY;
		try {
			packet.arg = query.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		lastTime = TimeUtil.currentTimeMillis();
		packet.write(this);
	}

	private static void getCharsetCommand(StringBuilder sb, int clientCharIndex) {
		sb.append("SET names ").append(CharsetUtil.getCharset(clientCharIndex))
				.append(";");
	}

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

	private void getAutocommitCommand(StringBuilder sb, boolean autoCommit) {
		if (autoCommit) {
			sb.append("SET autocommit=1;");
		} else {
			sb.append("SET autocommit=0;");
		}
	}

	private static class StatusSync {
		private final String schema;
		private final Integer charsetIndex;
		private final Integer txtIsolation;
		private final Boolean autocommit;
		private final AtomicInteger synCmdCount;
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

		public boolean synAndExecuted(MySQLConnection conn) {
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

		private void updateConnectionInfo(MySQLConnection conn)

		{
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

	/**
	 * @return if synchronization finished and execute-sql has already been sent
	 *         before
	 */
	public boolean syncAndExcute() {
		StatusSync sync = this.statusSync;
		if (sync == null) {
			return true;
		} else {
			boolean executed = sync.synAndExecuted(this);
			if (executed) {
				statusSync = null;
			}
			return executed;
		}

	}

	public void execute(RouteResultsetNode rrn, ServerConnection sc,
			boolean autocommit) throws UnsupportedEncodingException {
		if (!modifiedSQLExecuted && rrn.isModifySQL()) {
			modifiedSQLExecuted = true;
		}
		String xaTXID = sc.getSession2().getXaTXID();
		synAndDoExecute(xaTXID, rrn, sc.getCharsetIndex(), sc.getTxIsolation(),
				autocommit);
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
		if (expectAutocommit == false && xaTxID != null && xaStatus == TxState.TX_INITIALIZE_STATE) {
			//clientTxIsoLation = Isolations.SERIALIZABLE;
			xaCmd = "XA START " + xaTxID + ';';
			this.xaStatus = TxState.TX_STARTED_STATE;
		}
		int schemaSyn = conSchema.equals(oldSchema) ? 0 : 1;
		int charsetSyn = 0;
		if (this.charsetIndex != clientCharSetIndex) {
			//need to syn the charset of connection.
			//set current connection charset to client charset.
			//otherwise while sending commend to server the charset will not coincidence.
			setCharset(CharsetUtil.getCharset(clientCharSetIndex));
			charsetSyn = 1;
		}
		int txIsoLationSyn = (txIsolation == clientTxIsoLation) ? 0 : 1;
		int autoCommitSyn = (conAutoComit == expectAutocommit) ? 0 : 1;
		int synCount = schemaSyn + charsetSyn + txIsoLationSyn + autoCommitSyn;
		if (synCount == 0 && this.xaStatus != TxState.TX_STARTED_STATE) {
			// not need syn connection
			sendQueryCmd(rrn.getStatement());
			return;
		}
		CommandPacket schemaCmd = null;
		StringBuilder sb = new StringBuilder();
		if (schemaSyn == 1) {
			schemaCmd = getChangeSchemaCommand(conSchema);
			// getChangeSchemaCommand(sb, conSchema);
		}

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
					+ (schemaCmd != null) + " con:" + this);
		}
		metaDataSyned = false;
		statusSync = new StatusSync(xaCmd != null, conSchema,
				clientCharSetIndex, clientTxIsoLation, expectAutocommit,
				synCount);
		// syn schema
		if (schemaCmd != null) {
			schemaCmd.write(this);
		}
		// and our query sql to multi command at last
		sb.append(rrn.getStatement()+";");
		// syn and execute others
		this.sendQueryCmd(sb.toString());
		// waiting syn result...

	}

	private static CommandPacket getChangeSchemaCommand(String schema) {
		CommandPacket cmd = new CommandPacket();
		cmd.packetId = 0;
		cmd.command = MySQLPacket.COM_INIT_DB;
		cmd.arg = schema.getBytes();
		return cmd;
	}

	/**
	 * by wuzh ,execute a query and ignore transaction settings for performance
	 * 
	 * @param sql
	 * @throws UnsupportedEncodingException
	 */
	public void query(String query) throws UnsupportedEncodingException {
		RouteResultsetNode rrn = new RouteResultsetNode("default",
				ServerParse.SELECT, query);

		synAndDoExecute(null, rrn, this.charsetIndex, this.txIsolation, true);

	}

	public long getLastTime() {
		return lastTime;
	}

	public void setLastTime(long lastTime) {
		this.lastTime = lastTime;
	}

	public void quit() {
		if (isQuit.compareAndSet(false, true) && !isClosed()) {
			if (isAuthenticated) {
				write(writeToBuffer(QuitPacket.QUIT, allocate()));
				write(allocate());
			} else {
				close("normal");
			}
		}
	}

	@Override
	public void close(String reason) {
		if (!isClosed.get()) {
			isQuit.set(true);
			super.close(reason);
			pool.connectionClosed(this);
			if (this.respHandler != null) {
				this.respHandler.connectionClose(this, reason);
				respHandler = null;
			}
		}
	}

	public void commit() {

		_COMMIT.write(this);

	}

	public boolean batchCmdFinished() {
		batchCmdCount--;
		return (batchCmdCount == 0);
	}

	public void execCmd(String cmd) {
		this.sendQueryCmd(cmd);
	}

	public void execBatchCmd(String[] batchCmds) {
		// "XA END "+xaID+";"+"XA PREPARE "+xaID
		this.batchCmdCount = batchCmds.length;
		StringBuilder sb = new StringBuilder();
		for (String sql : batchCmds) {
			sb.append(sql).append(';');
		}
		this.sendQueryCmd(sb.toString());
	}

	public void rollback() {
		_ROLLBACK.write(this);
	}

	public void release() {
		if (metaDataSyned == false) {// indicate connection not normalfinished
										// ,and
										// we can't know it's syn status ,so
										// close
										// it
			LOGGER.warn("can't sure connection syn result,so close it " + this);
			this.respHandler = null;
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

	public boolean setResponseHandler(ResponseHandler queryHandler) {
		if (handler instanceof MySQLConnectionHandler) {
			((MySQLConnectionHandler) handler).setResponseHandler(queryHandler);
			respHandler = queryHandler;
			return true;
		} else if (queryHandler != null) {
			LOGGER.warn("set not MySQLConnectionHandler "
					+ queryHandler.getClass().getCanonicalName());
		}
		return false;
	}

	/**
	 * 写队列为空，可以继续写数据
	 */
	public void writeQueueAvailable() {
		if (respHandler != null) {
			respHandler.writeQueueAvailable();
		}
	}

	/**
	 * 记录sql执行信息
	 */
	public void recordSql(String host, String schema, String stmt) {
		// final long now = TimeUtil.currentTimeMillis();
		// if (now > this.lastTime) {
		// // long time = now - this.lastTime;
		// // SQLRecorder sqlRecorder = this.pool.getSqlRecorder();
		// // if (sqlRecorder.check(time)) {
		// // SQLRecord recorder = new SQLRecord();
		// // recorder.host = host;
		// // recorder.schema = schema;
		// // recorder.statement = stmt;
		// // recorder.startTime = lastTime;
		// // recorder.executeTime = time;
		// // recorder.dataNode = pool.getName();
		// // recorder.dataNodeIndex = pool.getIndex();
		// // sqlRecorder.add(recorder);
		// // }
		// }
		// this.lastTime = now;
	}

	private static byte[] passwd(String pass, HandshakePacket hs)
			throws NoSuchAlgorithmException {
		if (pass == null || pass.length() == 0) {
			return null;
		}
		byte[] passwd = pass.getBytes();
		int sl1 = hs.seed.length;
		int sl2 = hs.restOfScrambleBuff.length;
		byte[] seed = new byte[sl1 + sl2];
		System.arraycopy(hs.seed, 0, seed, 0, sl1);
		System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
		return SecurityUtil.scramble411(passwd, seed);
	}

	@Override
	public boolean isFromSlaveDB() {
		return fromSlaveDB;
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
	public String toString() {
		return "MySQLConnection [id=" + id + ", lastTime=" + lastTime
				+ ", user=" + user
				+ ", schema=" + schema + ", old shema=" + oldSchema
				+ ", borrowed=" + borrowed + ", fromSlaveDB=" + fromSlaveDB
				+ ", threadId=" + threadId + ", charset=" + charset
				+ ", txIsolation=" + txIsolation + ", autocommit=" + autocommit
				+ ", attachment=" + attachment + ", respHandler=" + respHandler
				+ ", host=" + host + ", port=" + port + ", statusSync="
				+ statusSync + ", writeQueue=" + this.getWriteQueue().size()
				+ ", modifiedSQLExecuted=" + modifiedSQLExecuted + "]";
	}

	@Override
	public boolean isModifiedSQLExecuted() {
		return modifiedSQLExecuted;
	}

	@Override
	public int getTxIsolation() {
		return txIsolation;
	}

}
