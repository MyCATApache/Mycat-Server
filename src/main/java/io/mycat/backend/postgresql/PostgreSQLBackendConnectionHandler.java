package io.mycat.backend.postgresql;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.utils.PacketUtils;
import io.mycat.net.NIOHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLBackendConnectionHandler implements NIOHandler<PostgreSQLBackendConnection> {

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLBackendConnectionHandler.class);

	@Override
	public void onConnected(PostgreSQLBackendConnection con) throws IOException {
		ByteBuffer buffer = PacketUtils.makeStartUpPacket(con.getUser(), con.getSchema());
		buffer.flip();
		LOGGER.debug("尝试发送启动包信息"); 
		con.getChannel().write(buffer);
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
