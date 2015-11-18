package io.mycat.backend.postgresql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.utils.PIOUtils;
import io.mycat.backend.postgresql.utils.PacketUtils;
import io.mycat.net.NIOHandler;

public class PostgreSQLBackendConnectionHandler implements NIOHandler<PostgreSQLBackendConnection> {

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLBackendConnectionHandler.class);

	@Override
	public void onConnected(PostgreSQLBackendConnection con) throws IOException {
		List<String[]> paramList = new ArrayList<String[]>();
		String user = "postgres";
		String password = "coollf";
		String database = "mycat";
		String appName = "MyCat-Server";

		paramList.add(new String[] { "user", user });
		paramList.add(new String[] { "database", database });
		paramList.add(new String[] { "client_encoding", "UTF8" });
		paramList.add(new String[] { "DateStyle", "ISO" });
		paramList.add(new String[] { "TimeZone", PacketUtils.createPostgresTimeZone() });
		paramList.add(new String[] { "extra_float_digits", "3" });
		paramList.add(new String[] { "application_name", appName });
		String[][] params = paramList.toArray(new String[0][]);
		if (LOGGER.isDebugEnabled()) {
			StringBuilder details = new StringBuilder();
			for (int i = 0; i < params.length; ++i) {
				if (i != 0)
					details.append(", ");
				details.append(params[i][0]);
				details.append("=");
				details.append(params[i][1]);
			}
			LOGGER.debug(" FE=> StartupPacket(" + details + ")");
		}

		/*
		 * Precalculate message length and encode params.
		 */
		int length = 4 + 4;
		byte[][] encodedParams = new byte[params.length * 2][];
		for (int i = 0; i < params.length; ++i) {
			encodedParams[i * 2] = params[i][0].getBytes("UTF-8");
			encodedParams[i * 2 + 1] = params[i][1].getBytes("UTF-8");
			length += encodedParams[i * 2].length + 1 + encodedParams[i * 2 + 1].length + 1;
		}

		length += 1; // Terminating \0

		ByteBuffer buffer = ByteBuffer.allocate(length);

		/*
		 * Send the startup message.
		 */
		PIOUtils.SendInteger4(length, buffer);
		PIOUtils.SendInteger2(3, buffer); // protocol major
		PIOUtils.SendInteger2(0, buffer); // protocol minor
		for (byte[] encodedParam : encodedParams) {
			PIOUtils.Send(encodedParam, buffer);
			PIOUtils.SendChar(0, buffer);
		}

		con.write(buffer);
	}

	@Override
	public void onConnectFailed(PostgreSQLBackendConnection con, Throwable e) {
	}

	@Override
	public void onClosed(PostgreSQLBackendConnection con, String reason) {
	}

	@Override
	public void handle(PostgreSQLBackendConnection con, ByteBuffer buf, int start, int readedLength) {
		switch (con.getState()) {
		case connecting: {
			doConnecting(con, buf, start, readedLength);
			return;
		}
		case connected: {
			try {
				doHandleBusinessMsg(con, buf, start, readedLength);
			} catch (Exception e) {
				LOGGER.warn("caught err of con " + con, e);
			}
			return;
		}

		default:
			LOGGER.warn("not handled connecton state  err " + con.getState() + " for con " + con);
			break;

		}
	}

	/***
	 * 进行业务处理
	 * 
	 * @param con
	 * @param buf
	 * @param start
	 * @param readedLength
	 */
	private void doHandleBusinessMsg(PostgreSQLBackendConnection con, ByteBuffer buf, int start, int readedLength) {
	}

	/***
	 * 进行连接处理
	 * 
	 * @param con
	 * @param buf
	 * @param start
	 * @param readedLength
	 */
	private void doConnecting(PostgreSQLBackendConnection con, ByteBuffer buf, int start, int readedLength) {
		//TODO 数据库用户名密码
		String user = "postgres";
		String password = "coollf";
		
		byte[] data = new byte[readedLength];
		buf.position(start);
		buf.get(data, 0, readedLength);
		try {
			List<PostgreSQLPacket> packets = PacketUtils.parsePacket(data, 0, readedLength);
			if (!packets.isEmpty()) {
				if (packets.get(0) instanceof AuthenticationPacket) {// pg认证信息
					AuthenticationPacket packet = (AuthenticationPacket) packets.get(0);
					AuthType aut = packet.getAuthType();
					if (aut != AuthType.Ok) {
						PasswordMessage pak = new PasswordMessage(user, password, aut,
								((AuthenticationPacket) packet).getSalt());
						ByteBuffer buffer = ByteBuffer.allocate(pak.getLength() + 1);
						pak.write(buffer);
						con.write(buffer);
					}else{//登入成功了....
						LOGGER.info("PostgreSQL 登入成功");
						
					}
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
