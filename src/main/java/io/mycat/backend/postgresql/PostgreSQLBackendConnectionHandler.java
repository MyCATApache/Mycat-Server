package io.mycat.backend.postgresql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.BackendKeyData;
import io.mycat.backend.postgresql.packet.CommandComplete;
import io.mycat.backend.postgresql.packet.DataRow;
import io.mycat.backend.postgresql.packet.ErrorResponse;
import io.mycat.backend.postgresql.packet.NoticeResponse;
import io.mycat.backend.postgresql.packet.ParameterStatus;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.packet.ReadyForQuery;
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

public class PostgreSQLBackendConnectionHandler implements NIOHandler<PostgreSQLBackendConnection> {

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLBackendConnectionHandler.class);
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
			System.err.println(JSON.toJSONString(packets));
			int _length = packets.size();
			if (_length > 0) {
				for (int offset = 0; offset < _length;) {
					PostgreSQLPacket head = packets.get(offset);
					if (head instanceof RowDescription) {
						List<PostgreSQLPacket> pgs = new ArrayList<>();
						List<PostgreSQLPacket> comp = new ArrayList<>();
						pgs.add(head);
						// 查询结包
						for (int i = offset + 1; i < _length; i++) {
							if (packets.get(i) instanceof DataRow) {
								pgs.add(packets.get(i));
							} else if (packets.get(i) instanceof CommandComplete) {
								CommandComplete com = (CommandComplete) packets.get(i);
								if (com.isSelectComplete()) {
									break;
								}
								comp.add(packets.get(i));
							} else {
								break;
							}
						}

						if (comp.size() > 0) {
							doHandleBusinessDDL(con, comp);
							offset = offset + comp.size();
						}

						offset = offset + pgs.size();
						doHandleBusinessQuery(con, pgs);
					} else if (head instanceof CommandComplete) {
						List<PostgreSQLPacket> pgs = new ArrayList<>();
						pgs.add(head);
						offset = offset + pgs.size();
						doHandleBusinessDDL(con, pgs);
					} else if (head instanceof NoticeResponse) {
						List<PostgreSQLPacket> pgs = new ArrayList<>();
						pgs.add(head);
						offset = offset + pgs.size();
						doHandleBusinessNotice(con, pgs);
					} else if (head instanceof ErrorResponse) {
						List<PostgreSQLPacket> pgs = new ArrayList<>();
						pgs.add(head);
						offset = offset + pgs.size();
						doHandleBusinessError(con, pgs);
					} else if (head instanceof ReadyForQuery) {
						List<PostgreSQLPacket> pgs = new ArrayList<>();
						pgs.add(head);
						offset = offset + pgs.size();
						doHandleBusinessReady(con, pgs);
					}else if( head instanceof ParameterStatus){
						offset ++;
					}
				}
			} else {
				ErrorPacket err = new ErrorPacket();
				err.packetId = ++packetId;
				err.message = "SQL 服务器处理出错!".getBytes();
				err.errno = ErrorCode.ERR_NOT_SUPPORTED;
				if (con.getResponseHandler() != null) {
					con.getResponseHandler().errorResponse(err.writeToBytes(), con);
				}
			}
		} catch (Exception e) {
			LOGGER.error("处理出异常了", e);
			ErrorPacket err = new ErrorPacket();
			err.packetId = ++packetId;
			err.message = ("内部服务器处理出错!" + e.getMessage()).getBytes();
			err.errno = ErrorCode.ERR_NOT_SUPPORTED;
			ResponseHandler respHand = con.getResponseHandler();
			if (respHand != null) {
				respHand.errorResponse(err.writeToBytes(), con);
			} else {
				System.err.println("respHand 不为空");
			}
		}
	}

	/**
	 * 后台已经完成了.
	 * 
	 * @param con
	 * @param pgs
	 */
	private void doHandleBusinessReady(PostgreSQLBackendConnection con, List<PostgreSQLPacket> pgs) {

	}

	/***
	 * 处理查询出错数据包
	 * 
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessError(PostgreSQLBackendConnection con, List<PostgreSQLPacket> packets) {
		LOGGER.debug("查询出错了!");
		ErrorResponse errMg = (ErrorResponse) packets.get(0);

		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.message = errMg.getErrMsg().trim().replaceAll("\0", " ").getBytes();
		err.errno = ErrorCode.ER_UNKNOWN_ERROR;
		con.getResponseHandler().errorResponse(err.writeToBytes(), con);

	}

	/***
	 * 数据操作语言
	 * 
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessDDL(PostgreSQLBackendConnection con, List<PostgreSQLPacket> packets) {
		for (CommandComplete cmdComplete : packets.toArray(new CommandComplete[0])) {
			if (cmdComplete.isSelectComplete()) {
				return;
			}
			if (cmdComplete.isDDLComplete()) {
				OkPacket okPck = new OkPacket();
				okPck.affectedRows = cmdComplete.getRows();
				okPck.insertId = 0;
				okPck.packetId = ++packetId;
				okPck.message = " OK!".getBytes();
				con.getResponseHandler().okResponse(okPck.writeToBytes(), con);
			} else if (cmdComplete.isTranComplete()) {
				OkPacket okPck = new OkPacket();
				okPck.affectedRows = cmdComplete.getRows();
				okPck.insertId = 0;
				okPck.packetId = ++packetId;
				okPck.message = cmdComplete.getCommandResponse().trim().getBytes();
				con.getResponseHandler().okResponse(okPck.writeToBytes(), con);
			} else {
				OkPacket okPck = new OkPacket();
				okPck.affectedRows = 0;
				okPck.insertId = 0;
				okPck.packetId = ++packetId;
				okPck.message = cmdComplete.getCommandResponse().trim().getBytes();
				con.getResponseHandler().okResponse(okPck.writeToBytes(), con);
				LOGGER.warn("其他命令完成,暂时忽略:", JSON.toJSONString(cmdComplete));
			}
		}
	}

	/******
	 * 执行成功但是又警告信息
	 * 
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessNotice(PostgreSQLBackendConnection con, List<PostgreSQLPacket> packets) {

	}

	/*****
	 * 处理简单查询
	 * 
	 * @param con
	 * @param packets
	 */
	private void doHandleBusinessQuery(PostgreSQLBackendConnection con, List<PostgreSQLPacket> packets) {
		RowDescription rowHd = (RowDescription) packets.get(0);
		List<FieldPacket> fieldPks = PgPacketApaterUtils.rowDescConvertFieldPacket(rowHd);
		List<RowDataPacket> rowDatas = new ArrayList<>();
		CommandComplete cmdComplete = null;
		for (int i = 1; i < packets.size(); i++) {
			PostgreSQLPacket _packet = packets.get(i);
			if (_packet instanceof DataRow) {
				rowDatas.add(PgPacketApaterUtils.rowDataConvertRowDataPacket((DataRow) _packet));
			} else if (_packet instanceof CommandComplete) {
				cmdComplete = (CommandComplete) _packet;
			} else {
				LOGGER.warn("unexpectedly PostgreSQLPacket:", JSON.toJSONString(_packet));
			}
		}

		BufferArray bufferArray = NetSystem.getInstance().getBufferPool().allocateArray();
		ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
		headerPkg.fieldCount = fieldPks.size();
		headerPkg.packetId = ++packetId;
		headerPkg.write(bufferArray);

		byte[] header = bufferArray.writeToByteArrayAndRecycle();

		List<byte[]> fields = new ArrayList<byte[]>(fieldPks.size());
		Iterator<FieldPacket> itor = fieldPks.iterator();
		while (itor.hasNext()) {
			bufferArray = NetSystem.getInstance().getBufferPool().allocateArray();
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
		if (con.getResponseHandler() != null) {
			con.getResponseHandler().fieldEofResponse(header, fields, eof, con);
		} else {
			LOGGER.error("响应句柄为空");
		}
		// output row
		for (RowDataPacket curRow : rowDatas) {
			bufferArray = NetSystem.getInstance().getBufferPool().allocateArray();
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
		if (con.getResponseHandler() != null) {
			con.getResponseHandler().rowEofResponse(eof, con);
		} else {
			LOGGER.error("响应句柄为空");
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
					} else {// 登入成功了....

						for (int i = 1; i < packets.size(); i++) {
							PostgreSQLPacket _p = packets.get(i);
							if (_p instanceof BackendKeyData) {
								con.setServerSecretKey(((BackendKeyData) _p).getSecretKey());
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
