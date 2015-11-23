package io.mycat.backend.postgresql;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.BackendKeyData;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.utils.PacketUtils;
import io.mycat.net.Connection.State;
import io.mycat.net.NIOHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class PostgreSQLBackendConnectionHandler implements NIOHandler<PostgreSQLBackendConnection> {

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLBackendConnectionHandler.class);

	@Override
	public void onConnected(PostgreSQLBackendConnection con) throws IOException {
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
		byte[] data = new byte[readedLength];
		buf.position(start);
		buf.get(data, 0, readedLength);
		try {
			List<PostgreSQLPacket> packets = PacketUtils.parsePacket(data, 0, readedLength);
			//处理数据 包将数据包做返回给前台
			//[{"columnNumber":2,"columns":[{"atttypmod":-1,"coid":0,"columnName":"@@character_set_database\u0000","columnType":"UNKNOWN","oid":0,"protocol":"TEXT","typlen":-2},{"atttypmod":-1,"coid":0,"columnName":"@@collation_database\u0000","columnType":"UNKNOWN","oid":0,"protocol":"TEXT","typlen":-2}],"length":88,"marker":"T","packetSize":89,"type":"RowDescription"},{"columnNumber":2,"columns":[{"data":"dXRmOA==","length":4,"null":false},{"data":"dXRmOF9nZW5lcmFsX2Np","length":15,"null":false}],"length":33,"marker":"D","packetSize":34,"type":"DataRow"},{"commandResponse":"SELECT 1\u0000","length":13,"marker":"C","packetSize":14,"type":"CommandComplete"},{"length":5,"marker":"Z","packetSize":6,"type":"ReadyForQuery"}]
			//con.getResponseHandler().rowResponse(row, conn);
			System.out.println("响应前台sql:"+ JSON.toJSONString(packets));
		} catch (IOException e) {
			con.getResponseHandler().errorResponse("出错了!".getBytes(), con);
		}
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
						PasswordMessage pak = new PasswordMessage(con.getUser(), con.getPassword(), aut,
								((AuthenticationPacket) packet).getSalt());
						ByteBuffer buffer = ByteBuffer.allocate(pak.getLength() + 1);
						pak.write(buffer);
						con.write(buffer);
					}else{//登入成功了....
						
						for(int i=1;i<packets.size();i++){
							PostgreSQLPacket _p = packets.get(i);
							if(_p instanceof BackendKeyData){
								con.setServerSecretKey(((BackendKeyData) _p).getSecretKey());
							}
						}
						LOGGER.info("PostgreSQL 登入成功");
						con.setState(State.connected);
						con.getResponseHandler().connectionAcquired(con);//连接已经可以用来
					}
					LOGGER.debug(JSON.toJSONString(packets));
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
