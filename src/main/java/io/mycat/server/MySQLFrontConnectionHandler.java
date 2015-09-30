package io.mycat.server;

import io.mycat.MycatServer;
import io.mycat.net.Connection;
import io.mycat.net.NIOHandler;
import io.mycat.server.packet.AuthPacket;
import io.mycat.server.packet.MySQLMessage;
import io.mycat.server.packet.MySQLPacket;
import io.mycat.server.packet.QuitPacket;
import io.mycat.server.packet.util.SecurityUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLFrontConnectionHandler implements
		NIOHandler<MySQLFrontConnection> {
	private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2,
			0, 0, 0 };
	protected static final Logger LOGGER = LoggerFactory
			.getLogger(MySQLFrontConnectionHandler.class);

	@Override
	public void onConnected(MySQLFrontConnection con) throws IOException {
		con.sendAuthPackge();
	}

	@Override
	public void handle(MySQLFrontConnection con, ByteBuffer data,
			final int start, final int readedLength) {
		switch (con.getState()) {
		case connecting: {
			doConnecting(con, data, start, readedLength);
			return;
		}
		case connected: {
			try {
				doHandleBusinessMsg(con, data, start, readedLength);
			} catch (Exception e) {
				LOGGER.warn("caught err ", e);
			}
			return;
		}

		default:
			LOGGER.warn("not handled connecton state  err " + con.getState()
					+ " for con " + con);
			break;

		}
	}

	private void doConnecting(MySQLFrontConnection source, ByteBuffer buf,
			final int start, final int readedLength) {
		byte[] data = new byte[readedLength];
		buf.get(data, start, readedLength);
		// check quit packet
		if (data.length == QuitPacket.QUIT.length
				&& data[4] == MySQLPacket.COM_QUIT) {
			source.close("quit packet");
			return;
		}

		AuthPacket auth = new AuthPacket();
		auth.read(data);

		// check user
		if (!checkUser(source, auth.user, source.getHost())) {
			failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR,
					"Access denied for user '" + auth.user + "'");
			return;
		}

		// check password
		if (!checkPassword(source, auth.password, auth.user)) {
			failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR,
					"Access denied for user '" + auth.user + "'");
			return;
		}

		// check schema
		switch (checkSchema(source, auth.database, auth.user)) {
		case ErrorCode.ER_BAD_DB_ERROR:
			failure(source, ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
					+ auth.database + "'");
			break;
		case ErrorCode.ER_DBACCESS_DENIED_ERROR:
			String s = "Access denied for user '" + auth.user
					+ "' to database '" + auth.database + "'";
			failure(source, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
			break;
		default:
			success(source, auth);
		}
	}

	protected boolean checkUser(MySQLFrontConnection source, String user,
			String host) {
		return source.getPrivileges().userExists(user, host);
	}

	protected boolean checkPassword(MySQLFrontConnection source,
			byte[] password, String user) {
		String pass = source.getPrivileges().getPassword(user);

		// check null
		if (pass == null || pass.length() == 0) {
			if (password == null || password.length == 0) {
				return true;
			} else {
				return false;
			}
		}
		if (password == null || password.length == 0) {
			return false;
		}

		// encrypt
		byte[] encryptPass = null;
		try {
			encryptPass = SecurityUtil.scramble411(pass.getBytes(),
					source.getSeed());
		} catch (NoSuchAlgorithmException e) {
			LOGGER.warn(source.toString(), e);
			return false;
		}
		if (encryptPass != null && (encryptPass.length == password.length)) {
			int i = encryptPass.length;
			while (i-- != 0) {
				if (encryptPass[i] != password[i]) {
					return false;
				}
			}
		} else {
			return false;
		}

		return true;
	}

	protected int checkSchema(MySQLFrontConnection source, String schema,
			String user) {
		if (schema == null) {
			return 0;
		}
		FrontendPrivileges privileges = source.getPrivileges();
		if (!privileges.schemaExists(schema)) {
			return ErrorCode.ER_BAD_DB_ERROR;
		}
		Set<String> schemas = privileges.getUserSchemas(user);
		if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
			return 0;
		} else {
			return ErrorCode.ER_DBACCESS_DENIED_ERROR;
		}
	}

	protected void success(MySQLFrontConnection source, AuthPacket auth) {
		source.setAuthenticated(true);
		source.setUser(auth.user);
		source.setSchema(auth.database);
		source.setCharsetIndex(auth.charsetIndex);
		if (LOGGER.isInfoEnabled()) {
			StringBuilder s = new StringBuilder();
			s.append(source).append('\'').append(auth.user)
					.append("' login success");
			byte[] extra = auth.extra;
			if (extra != null && extra.length > 0) {
				s.append(",extra:").append(new String(extra));
			}
			LOGGER.info(s.toString());
		}

		source.write(AUTH_OK);
		boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.clientFlags);
		boolean usingCompress = MycatServer.getInstance().getConfig()
				.getSystem().getUseCompression() == 1;
		if (clientCompress && usingCompress) {
			source.setSupportCompress(true);
		}
		source.setState(Connection.State.connected);
	}

	protected void failure(MySQLFrontConnection source, int errno, String info) {
		LOGGER.error(source.toString() + info);
		source.writeErrMessage(errno, info);
	}

	public void onClosed(MySQLFrontConnection con,String reason) {

	}

	public void doHandleBusinessMsg(final MySQLFrontConnection source,
			final ByteBuffer buf, final int start, final int readedLength) {
		byte[] data = new byte[readedLength];
		buf.get(data, start, readedLength);
		if (source.getLoadDataInfileHandler() != null
				&& source.getLoadDataInfileHandler().isStartLoadData()) {
			MySQLMessage mm = new MySQLMessage(data);
			int packetLength = mm.readUB3();
			if (packetLength + 4 == data.length) {
				source.loadDataInfileData(data);
			}
			return;
		}
		switch (data[4]) {
		case MySQLPacket.COM_INIT_DB:
			source.initDB(data);
			break;
		case MySQLPacket.COM_QUERY:
			source.query(data);
			break;
		case MySQLPacket.COM_PING:
			source.ping();
			break;
		case MySQLPacket.COM_QUIT:
			source.close("quit cmd");
			break;
		case MySQLPacket.COM_PROCESS_KILL:
			source.kill(data);
			break;
		case MySQLPacket.COM_STMT_PREPARE:
			source.stmtPrepare(data);
			break;
		case MySQLPacket.COM_STMT_EXECUTE:

			source.stmtExecute(data);
			break;
		case MySQLPacket.COM_STMT_CLOSE:

			source.stmtClose(data);
			break;
		case MySQLPacket.COM_HEARTBEAT:

			source.heartbeat(data);
			break;
		default:
			source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"Unknown command");

		}

	}

	@Override
	public void onConnectFailed(MySQLFrontConnection con, Throwable e) {

	}

}