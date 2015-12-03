package io.mycat.backend.postgresql;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.BackendKeyData;
import io.mycat.backend.postgresql.packet.CommandComplete;
import io.mycat.backend.postgresql.packet.DataRow;
import io.mycat.backend.postgresql.packet.ErrorResponse;
import io.mycat.backend.postgresql.packet.NoticeResponse;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.packet.ReadyForQuery;
import io.mycat.backend.postgresql.packet.ReadyForQuery.TransactionState;
import io.mycat.backend.postgresql.packet.RowDescription;
import io.mycat.backend.postgresql.utils.PacketUtils;
import io.mycat.backend.postgresql.utils.PgPacketApaterUtils;
import io.mycat.net.BufferArray;
import io.mycat.net.Connection.State;
import io.mycat.net.NIOHandler;
import io.mycat.net.NetSystem;
import io.mycat.server.ErrorCode;
import io.mycat.server.executors.ResponseHandler;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.ErrorPacket;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.OkPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class PostgreSQLBackendConnectionHandler implements
		NIOHandler<PostgreSQLBackendConnection> {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(PostgreSQLBackendConnectionHandler.class);
	private byte packetId;

	@Override
	public void onConnected(PostgreSQLBackendConnection con) throws IOException {
	}

	@Override
	public void onConnectFailed(PostgreSQLBackendConnection source, Throwable e) {
		ResponseHandler respHand = source.getResponseHandler();
		if (respHand != null) {
			respHand.connectionError(e, source);
		}
	}

	@Override
	public void onClosed(PostgreSQLBackendConnection source, String reason) {
		ResponseHandler respHand = source.getResponseHandler();
		if (respHand != null) {
			respHand.connectionClose(source, reason);
		}
	}

	@Override
	public void handle(PostgreSQLBackendConnection con, ByteBuffer buf,
			int start, int readedLength) {
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
			LOGGER.warn("not handled connecton state  err " + con.getState()
					+ " for con " + con);
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
	private void doHandleBusinessMsg(PostgreSQLBackendConnection con,
			ByteBuffer buf, int start, int readedLength) {
		byte[] data = new byte[readedLength];
		buf.position(start);
		buf.get(data, 0, readedLength);
		try {
			List<PostgreSQLPacket> packets = PacketUtils.parsePacket(data, 0,
					readedLength);
			if (packets.size() > 0) {
				if (packets.get(0) instanceof RowDescription) {// SELECT sql
					doHandleBusinessQuery(con, packets);
					return;
				} else if (packets.get(0) instanceof CommandComplete) {// DDL
																		// DML
																		// 相关sql
																		// 执行
					doHandleBusinessDDL(con, packets);
					return;
				} else if (packets.get(0) instanceof NoticeResponse){
					doHandleBusinessNotice(con, packets);
				} else if (packets.get(0) instanceof ErrorResponse) {// 查询语句句出错了
					doHandleBusinessError(con, packets);
					return;
				}
			}
			ErrorPacket err = new ErrorPacket();
			err.packetId = ++packetId;
			err.message = "SQL 服务器处理出错!".getBytes();
			err.errno = ErrorCode.ERR_NOT_SUPPORTED;
			con.getResponseHandler().errorResponse(err.writeToBytes(), con);
		} catch (Exception e) {
			LOGGER.error("处理出异常了", e);
			ErrorPacket err = new ErrorPacket();
			err.packetId = ++packetId;
			err.message = ("内部服务器处理出错!" + e.getMessage()).getBytes();
			err.errno = ErrorCode.ERR_NOT_SUPPORTED;
			con.getResponseHandler().errorResponse(err.writeToBytes(), con);
		}
	}

	

	/***
	 * 处理查询出错数据包
	 * 
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessError(PostgreSQLBackendConnection con,
			List<PostgreSQLPacket> packets) {
		LOGGER.debug("查询出错了!");
		ErrorResponse errMg = (ErrorResponse) packets.get(0);
		ReadyForQuery readyForQuery = null;
		for (PostgreSQLPacket _pk : packets) {
			if (_pk instanceof ReadyForQuery) {
				readyForQuery = (ReadyForQuery) _pk;
			}
		}

		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.message = errMg.getErrMsg().trim().replaceAll("\0", " ").getBytes();
		err.errno = ErrorCode.ER_UNKNOWN_ERROR;
		con.getResponseHandler().errorResponse(err.writeToBytes(), con);
		if (readyForQuery == null) {
			LOGGER.error("此sql出现错误,造成后端连接不能用了");
		}
	}

	/***
	 * 数据操作语言
	 * 
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessDDL(PostgreSQLBackendConnection con,
			List<PostgreSQLPacket> packets) {
		CommandComplete cmdComplete = (CommandComplete) packets.get(0);

		ReadyForQuery readyForQuery = null;
		for (PostgreSQLPacket packet : packets) {
			if (packet instanceof ReadyForQuery) {
				readyForQuery = (ReadyForQuery) packet;
			}
		}
		if (readyForQuery != null) {
			if ((readyForQuery.getState() == TransactionState.IN) != con
					.isInTransaction()) {
				con.setInTransaction((readyForQuery.getState() == TransactionState.IN));
			}
		}

		OkPacket okPck = new OkPacket();
		okPck.affectedRows = cmdComplete.getRows();
		okPck.insertId = 0;
		okPck.packetId = ++packetId;
		okPck.message = " OK!".getBytes();
		con.getResponseHandler().okResponse(okPck.writeToBytes(), con);
	}
	
	/******
	 * 执行成功但是又警告信息
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessNotice(PostgreSQLBackendConnection con,
			List<PostgreSQLPacket> packets) {
		NoticeResponse notice = (NoticeResponse) packets.get(0);		
		CommandComplete cmdComplete = null;
		ReadyForQuery readyForQuery = null;
		for (PostgreSQLPacket packet : packets) {
			if(packet instanceof CommandComplete){
				cmdComplete = (CommandComplete) packet;
			}
			if (packet instanceof ReadyForQuery) {
				readyForQuery = (ReadyForQuery) packet;
			}
		}
		if (readyForQuery != null) {
			if ((readyForQuery.getState() == TransactionState.IN) != con
					.isInTransaction()) {
				con.setInTransaction((readyForQuery.getState() == TransactionState.IN));
			}
		}

		OkPacket okPck = new OkPacket();
		okPck.affectedRows = cmdComplete.getRows();
		okPck.insertId = 0;
		okPck.packetId = ++packetId;
		String msg = notice.getMsg().replace("\0"," ") +"\n OK!";		
		okPck.message =  msg.getBytes();		
		con.getResponseHandler().okResponse(okPck.writeToBytes(), con);
	}

	/*****
	 * 处理简单查询
	 * 
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessQuery(PostgreSQLBackendConnection con,
			List<PostgreSQLPacket> packets) {
		RowDescription rowHd = (RowDescription) packets.get(0);
		List<FieldPacket> fieldPks = PgPacketApaterUtils
				.rowDescConvertFieldPacket(rowHd);
		List<RowDataPacket> rowDatas = new ArrayList<>();
		CommandComplete cmdComplete = null;
		ReadyForQuery readyForQuery = null;
		for (int i = 1; i < packets.size(); i++) {
			PostgreSQLPacket _packet = packets.get(i);
			if (_packet instanceof DataRow) {
				rowDatas.add(PgPacketApaterUtils
						.rowDataConvertRowDataPacket((DataRow) _packet));
			} else if (_packet instanceof CommandComplete) {
				cmdComplete = (CommandComplete) _packet;
			} else if (_packet instanceof ReadyForQuery) {
				readyForQuery = (ReadyForQuery) _packet;
			} else {
				LOGGER.warn("unexpectedly PostgreSQLPacket:",
						JSON.toJSONString(_packet));
			}
		}
		if (readyForQuery != null) {
			if ((readyForQuery.getState() == TransactionState.IN) != con
					.isInTransaction()) {
				con.setInTransaction((readyForQuery.getState() == TransactionState.IN));
			}
		}
		BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
				.allocateArray();
		ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
		headerPkg.fieldCount = fieldPks.size();
		headerPkg.packetId = ++packetId;
		headerPkg.write(bufferArray);

		byte[] header = bufferArray.writeToByteArrayAndRecycle();

		List<byte[]> fields = new ArrayList<byte[]>(fieldPks.size());
		Iterator<FieldPacket> itor = fieldPks.iterator();
		while (itor.hasNext()) {
			bufferArray = NetSystem.getInstance().getBufferPool()
					.allocateArray();
			FieldPacket curField = itor.next();
			curField.packetId = ++packetId;
			curField.write(bufferArray);
			byte[] field = bufferArray.writeToByteArrayAndRecycle();
			fields.add(field);
			itor.remove();
		}

		bufferArray = NetSystem.getInstance().getBufferPool().allocateArray();
		EOFPacket eofPckg = new EOFPacket();
		eofPckg.packetId = ++packetId;
		eofPckg.write(bufferArray);
		byte[] eof = bufferArray.writeToByteArrayAndRecycle();
		con.getResponseHandler().fieldEofResponse(header, fields, eof, con);

		// output row
		for (RowDataPacket curRow : rowDatas) {
			bufferArray = NetSystem.getInstance().getBufferPool()
					.allocateArray();
			curRow.packetId = ++packetId;
			curRow.write(bufferArray);
			byte[] row = bufferArray.writeToByteArrayAndRecycle();
			con.getResponseHandler().rowResponse(row, con);
		}

		// end row
		bufferArray = NetSystem.getInstance().getBufferPool().allocateArray();
		eofPckg = new EOFPacket();
		eofPckg.packetId = ++packetId;
		eofPckg.write(bufferArray);
		eof = bufferArray.writeToByteArrayAndRecycle();
		con.getResponseHandler().rowEofResponse(eof, con);
	}

	/***
	 * 进行连接处理
	 * 
	 * @param con
	 * @param buf
	 * @param start
	 * @param readedLength
	 */
	private void doConnecting(PostgreSQLBackendConnection con, ByteBuffer buf,
			int start, int readedLength) {

		byte[] data = new byte[readedLength];
		buf.position(start);
		buf.get(data, 0, readedLength);
		try {
			List<PostgreSQLPacket> packets = PacketUtils.parsePacket(data, 0,
					readedLength);
			if (!packets.isEmpty()) {
				if (packets.get(0) instanceof AuthenticationPacket) {// pg认证信息
					AuthenticationPacket packet = (AuthenticationPacket) packets
							.get(0);
					AuthType aut = packet.getAuthType();
					if (aut != AuthType.Ok) {
						PasswordMessage pak = new PasswordMessage(
								con.getUser(), con.getPassword(), aut,
								((AuthenticationPacket) packet).getSalt());
						ByteBuffer buffer = ByteBuffer
								.allocate(pak.getLength() + 1);
						pak.write(buffer);
						con.write(buffer);
					} else {// 登入成功了....

						for (int i = 1; i < packets.size(); i++) {
							PostgreSQLPacket _p = packets.get(i);
							if (_p instanceof BackendKeyData) {
								con.setServerSecretKey(((BackendKeyData) _p)
										.getSecretKey());
							}
						}
						con.setState(State.connected);
						con.getResponseHandler().connectionAcquired(con);// 连接已经可以用来
					}
					LOGGER.debug(JSON.toJSONString(packets));
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
