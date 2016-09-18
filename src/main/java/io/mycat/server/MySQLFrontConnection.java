package io.mycat.server;

import io.mycat.MycatServer;
import io.mycat.net.NetSystem;
import io.mycat.route.RouteResultset;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.packet.HandshakePacket;
import io.mycat.server.packet.MySQLMessage;
import io.mycat.server.packet.OkPacket;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.sqlhandler.BeginHandler;
import io.mycat.server.sqlhandler.ExplainHandler;
import io.mycat.server.sqlhandler.KillHandler;
import io.mycat.server.sqlhandler.SavepointHandler;
import io.mycat.server.sqlhandler.SelectHandler;
import io.mycat.server.sqlhandler.ServerLoadDataInfileHandler;
import io.mycat.server.sqlhandler.ServerPrepareHandler;
import io.mycat.server.sqlhandler.SetHandler;
import io.mycat.server.sqlhandler.ShowHandler;
import io.mycat.server.sqlhandler.StartHandler;
import io.mycat.server.sqlhandler.UseHandler;
import io.mycat.util.RandomUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Set;

/**
 * MySQL Front connection
 *
 * @author wuzhih
 *
 */

public class MySQLFrontConnection extends GenalMySQLConnection {
	protected FrontendPrivileges privileges;

	protected FrontendPrepareHandler prepareHandler;
	protected LoadDataInfileHandler loadDataInfileHandler;
	private final NonBlockingSession session;
	private boolean readOnlyUser = false;

	protected int getServerCapabilities() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		boolean usingCompress = MycatServer.getInstance().getConfig()
				.getSystem().getUseCompression() == 1;
		if (usingCompress) {
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
		// flag |= ServerDefs.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		return flag;
	}

	public MySQLFrontConnection(SocketChannel channel) throws IOException {
		super(channel);

		session = new NonBlockingSession(this);
		InetSocketAddress remoteAddr = null;
		InetSocketAddress localAddr = (InetSocketAddress) channel
				.getLocalAddress();
		remoteAddr = (InetSocketAddress) ((SocketChannel) channel)
				.getRemoteAddress();
		this.host = remoteAddr.getHostString();
		this.port = remoteAddr.getPort();
		this.localPort = localAddr.getPort();
		loadDataInfileHandler = new ServerLoadDataInfileHandler(this);
		prepareHandler = new ServerPrepareHandler(this);
	}

	public void sendAuthPackge() throws IOException {
		// 生成认证数据
		byte[] rand1 = RandomUtil.randomBytes(8);
		byte[] rand2 = RandomUtil.randomBytes(12);

		// 保存认证数据
		byte[] seed = new byte[rand1.length + rand2.length];
		System.arraycopy(rand1, 0, seed, 0, rand1.length);
		System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
		this.seed = seed;

		// 发送握手数据包
		HandshakePacket hs = new HandshakePacket();
		hs.packetId = 0;
		hs.protocolVersion = Versions.PROTOCOL_VERSION;
		hs.serverVersion = Versions.SERVER_VERSION;
		hs.threadId = id;
		hs.seed = rand1;
		hs.serverCapabilities = getServerCapabilities();
		hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
		hs.serverStatus = 2;
		hs.restOfScrambleBuff = rand2;
		hs.write(this);

		// asynread response
		this.asynRead();
	}

	/**
	 * 设置是否需要中断当前事务
	 */
	public void setTxInterrupt(String txInterrputMsg) {
		if (!autocommit && !txInterrupted) {
			txInterrupted = true;
			this.txInterrputMsg = txInterrputMsg;
		}
	}

	public FrontendPrivileges getPrivileges() {
		return privileges;
	}

	public void setPrivileges(FrontendPrivileges privileges) {
		this.privileges = privileges;
	}

	public boolean isTxInterrupted() {
		return txInterrupted;
	}

	public NonBlockingSession getSession2() {
		return this.session;
	}

	public void initDB(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		mm.position(5);
		String db = mm.readString();

		// 检查schema的有效性
		if (db == null || !privileges.schemaExists(db)) {
			writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
					+ db + "'");
			return;
		}
		if (!privileges.userExists(user, host)) {
			writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR,
					"Access denied for user '" + user + "'");
			return;
		}
		readOnlyUser = privileges.isReadOnly(user);
		Set<String> schemas = privileges.getUserSchemas(user);
		if (schemas == null || schemas.size() == 0 || schemas.contains(db)) {
			this.schema = db;
			write(OkPacket.OK);
		} else {
			String s = "Access denied for user '" + user + "' to database '"
					+ db + "'";
			writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
		}

	}

	public void loadDataInfileStart(String sql) {
		if (loadDataInfileHandler != null) {
			try {
				loadDataInfileHandler.start(sql);
			} catch (Exception e) {
				LOGGER.error("load data error", e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
			}

		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"load data infile sql is not  unsupported!");
		}

	}

	public void loadDataInfileData(byte[] data) {
		if (loadDataInfileHandler != null) {
			try {
				loadDataInfileHandler.handle(data);
			} catch (Exception e) {
				LOGGER.error("load data error", e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
			}
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"load data infile  data is not  unsupported!");
		}

	}

	public void loadDataInfileEnd(byte packID) {
		if (loadDataInfileHandler != null) {
			try {
				loadDataInfileHandler.end(packID);
			} catch (Exception e) {
				LOGGER.error("load data error", e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
			}
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"load data infile end is not  unsupported!");
		}

	}

	public void query(byte[] data) {

		if (this.isClosed()) {
			LOGGER.warn("ignore execute ,server connection is closed " + this);
			return;
		}
		// 状态检查
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback." + txInterrputMsg);
			return;
		}

		// 取得语句
		MySQLMessage mm = new MySQLMessage(data);
		mm.position(5);
		String sql = null;
		try {
			sql = mm.readString(charset);
		} catch (UnsupportedEncodingException e) {
			writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
					"Unknown charset '" + charset + "'");
			return;
		}
		
		query(sql);

	}
	
	public void query(String sql) {
		if (sql == null || sql.length() == 0) {
			writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
			return;
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(new StringBuilder().append(this).append(" ")
					.append(sql).toString());
		}

		// sql = StringUtil.replace(sql, "`", "");

		// remove last ';'
		if (sql.endsWith(";")) {
			sql = sql.substring(0, sql.length() - 1);
		}

		// 执行查询
		int rs = ServerParse.parse(sql);
		int sqlType = rs & 0xff;

		// 检查当前使用的DB
		String db = this.schema;
		if (db == null
				&& sqlType!=ServerParse.USE
				&& sqlType!=ServerParse.HELP
				&& sqlType!=ServerParse.SET
				&& sqlType!=ServerParse.SHOW
				&& sqlType!=ServerParse.KILL
				&& sqlType!=ServerParse.KILL_QUERY
				&& sqlType!=ServerParse.MYSQL_COMMENT
				&& sqlType!=ServerParse.MYSQL_CMD_COMMENT) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No MyCAT Database selected");
			return;
		}

		switch (sqlType) {
		case ServerParse.EXPLAIN:
			ExplainHandler.handle(sql, this, rs >>> 8);
			break;
		case ServerParse.SET:
			SetHandler.handle(sql, this, rs >>> 8);
			break;
		case ServerParse.SHOW:
			ShowHandler.handle(sql, this, rs >>> 8);
			break;
		case ServerParse.SELECT:
			SelectHandler.handle(sql, this, rs >>> 8);
			break;
		case ServerParse.START:
			StartHandler.handle(sql, this, rs >>> 8);
			break;
		case ServerParse.BEGIN:
			BeginHandler.handle(sql, this);
			break;
		case ServerParse.SAVEPOINT:
			SavepointHandler.handle(sql, this);
			break;
		case ServerParse.KILL:
			KillHandler.handle(sql, rs >>> 8, this);
			break;
		case ServerParse.KILL_QUERY:
			LOGGER.warn(new StringBuilder().append("Unsupported command:")
					.append(sql).toString());
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"Unsupported command");
			break;
		case ServerParse.USE:
			UseHandler.handle(sql, this, rs >>> 8);
			break;
		case ServerParse.COMMIT:
			commit();
			break;
		case ServerParse.ROLLBACK:
			rollback();
			break;
		case ServerParse.HELP:
			LOGGER.warn(new StringBuilder().append("Unsupported command:")
					.append(sql).toString());
			writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
			break;
		case ServerParse.MYSQL_CMD_COMMENT:
			write(OkPacket.OK);
			break;
		case ServerParse.MYSQL_COMMENT:
			write(OkPacket.OK);
			break;
		case ServerParse.LOAD_DATA_INFILE_SQL:
			loadDataInfileStart(sql);
			break;
		default:
			if (this.isReadOnlyUser()) {
				LOGGER.warn(new StringBuilder().append("User readonly:")
						.append(sql).toString());
				writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User readonly");
				break;
			}
			execute(sql, rs & 0xff);
		}
	}

	public boolean isReadOnlyUser() {
		return readOnlyUser;
	}

	public void execute(String sql, int type) {
		SchemaConfig schema = MycatServer.getInstance().getConfig()
				.getSchemas().get(this.schema);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + schema + "'");
			return;
		}
		routeEndExecuteSQL(sql, type, schema);

	}

	public void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),
							schema, type, sql, this.charset, this);

		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(
					s.append(this).append(sql).toString() + " err:"
							+ e.toString(), e);
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e
					.getClass().getSimpleName() : msg);
			return;
		}
		if (rrs != null) {
			// session执行
			session.execute(rrs, type);
		}
	}

	public void stmtPrepare(byte[] data) {
		if (prepareHandler != null) {
			// 取得语句
			MySQLMessage mm = new MySQLMessage(data);
			mm.position(5);
			String sql = null;
			try {
				sql = mm.readString(charset);
			} catch (UnsupportedEncodingException e) {
				writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
						"Unknown charset '" + charset + "'");
				return;
			}
			if (sql == null || sql.length() == 0) {
				writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
				return;
			}

			// 执行预处理
			prepareHandler.prepare(sql);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"Prepare unsupported!");
		}
	}

	public void stmtExecute(byte[] data) {
		if (prepareHandler != null) {
			prepareHandler.execute(data);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"Prepare unsupported!");
		}
	}

	public void stmtClose(byte[] data) {
		if (prepareHandler != null) {
			prepareHandler.close(data);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"Prepare unsupported!");
		}
	}

	public RouteResultset routeSQL(String sql, int type) {

		// 检查当前使用的DB
		String db = this.schema;
		if (db == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"No MyCAT Database selected");
			return null;
		}
		SchemaConfig schema = MycatServer.getInstance().getConfig()
				.getSchemas().get(db);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + db + "'");
			return null;
		}

		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),
							schema, type, sql, this.charset, this);

		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(
					s.append(this).append(sql).toString() + " err:"
							+ e.toString(), e);
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e
					.getClass().getSimpleName() : msg);
			return null;
		}
		return rrs;
	}

	/**
	 * 提交事务
	 */
	public void commit() {
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback.");
		} else {
			session.commit();
		}
	}

	/**
	 * 回滚事务
	 */
	public void rollback() {
		// 状态检查
		if (txInterrupted) {
			txInterrupted = false;
		}

		// 执行回滚
		session.rollback();
	}

	/**
	 * 撤销执行中的语句
	 *
	 * @param sponsor
	 *            发起者为null表示是自己
	 */
	public void cancel(final MySQLFrontConnection sponsor) {
		NetSystem.getInstance().getExecutor().execute(new Runnable() {
			@Override
			public void run() {
				session.cancel(sponsor);
			}
		});
	}

	@Override
	public void close(String reason) {
		super.close(reason);
		session.terminate();
		if (getLoadDataInfileHandler() != null) {
			getLoadDataInfileHandler().clear();
		}
		if(getPrepareHandler() != null) {
			getPrepareHandler().clear();
		}
	}

	public LoadDataInfileHandler getLoadDataInfileHandler() {
		return loadDataInfileHandler;
	}
	
	public FrontendPrepareHandler getPrepareHandler() {
		return prepareHandler;
	}

	public void ping() {
		write(OkPacket.OK);
	}

	public void heartbeat(byte[] data) {
		write(OkPacket.OK);
	}

	public void kill(byte[] data) {
		writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
	}

	public void unknown(byte[] data) {
		writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
	}

}
