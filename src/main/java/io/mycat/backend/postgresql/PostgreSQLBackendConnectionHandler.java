package io.mycat.backend.postgresql;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.BackendKeyData;
import io.mycat.backend.postgresql.packet.CommandComplete;
import io.mycat.backend.postgresql.packet.CopyInResponse;
import io.mycat.backend.postgresql.packet.CopyOutResponse;
import io.mycat.backend.postgresql.packet.DataRow;
import io.mycat.backend.postgresql.packet.EmptyQueryResponse;
import io.mycat.backend.postgresql.packet.ErrorResponse;
import io.mycat.backend.postgresql.packet.NoticeResponse;
import io.mycat.backend.postgresql.packet.NotificationResponse;
import io.mycat.backend.postgresql.packet.ParameterStatus;
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
		try {
			List<PostgreSQLPacket> packets = PacketUtils.parsePacket(buf, 0, readedLength);
			if(packets== null || packets.isEmpty()){
				throw new RuntimeException("数据包解析出错");
			}
			SelectResponse response = null;
			for(PostgreSQLPacket packet: packets){
				if(packet instanceof ErrorResponse){
					doProcessErrorResponse(con,(ErrorResponse)packet);
				}else if(packet instanceof RowDescription){
					response = new SelectResponse((RowDescription) packet);
				}else if(packet instanceof DataRow){
					response.addDataRow((DataRow)packet);
				}else if(packet instanceof ParameterStatus){
					doProcessParameterStatus(con,(ParameterStatus)packet);
				}else if(packet instanceof CommandComplete){
					doProcessCommandComplete(con, (CommandComplete) packet,response);
				}else if(packet instanceof NoticeResponse){
					doProcessNoticeResponse(con, (NoticeResponse)packet);
				}else if(packet instanceof ReadyForQuery){
					doProcessReadyForQuery(con, (ReadyForQuery) packet);
				}else if(packet instanceof NotificationResponse){
					doProcessNotificationResponse(con,(NotificationResponse)packet);
				}else if(packet instanceof CopyInResponse){
					doProcessCopyInResponse(con,(CopyInResponse)packet);
				}else if(packet instanceof CopyOutResponse){
					doProcessCopyOutResponse(con,(CopyOutResponse)packet);
				}else if(packet instanceof EmptyQueryResponse){
					doProcessEmptyQueryResponse(con,(EmptyQueryResponse)packet);
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

	private void doProcessEmptyQueryResponse(PostgreSQLBackendConnection con,EmptyQueryResponse packet) {
		// TODO(现阶段无空白sql)
	}

	private void doProcessCopyOutResponse(PostgreSQLBackendConnection con,CopyOutResponse packet) {
		// TODO(复制数据暂时不需要)		
	}

	private void doProcessCopyInResponse(PostgreSQLBackendConnection con,CopyInResponse packet) {
		// TODO(复制数据暂时不需要)		
	}

	private void doProcessNotificationResponse(PostgreSQLBackendConnection con,NotificationResponse notificationResponse) {
		// TODO(后台参数改变通知)		
	}

	private void doProcessParameterStatus(PostgreSQLBackendConnection con,ParameterStatus parameterStatus) {		
		// TODO(设置参数响应)		
	}

	/**
	 * 后台已经完成了.
	 * 
	 * @param con
	 * @param packet
	 */
	private void doProcessReadyForQuery(PostgreSQLBackendConnection con, ReadyForQuery readyForQuery) {
		if(con.isInTransaction() != (readyForQuery.getState() == TransactionState.IN)){//设置连接的后台事物状态
			con.setInTransaction((readyForQuery.getState() == TransactionState.IN));
		}
	}
	
	/******
	 * 执行成功但是又警告信息
	 * 
	 * @param con
	 * @param packet
	 */
	private void doProcessNoticeResponse(PostgreSQLBackendConnection con, NoticeResponse noticeResponse) {
		//TODO (通知提醒信息)
	}

	/***
	 * 处理查询出错数据包
	 * 
	 * @param con
	 * @param errMg
	 */
	private void doProcessErrorResponse(PostgreSQLBackendConnection con, ErrorResponse errorResponse) {
		LOGGER.debug("查询出错了!");
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.message = errorResponse.getErrMsg().trim().replaceAll("\0", " ").getBytes();
		err.errno = ErrorCode.ER_UNKNOWN_ERROR;
		con.getResponseHandler().errorResponse(err.writeToBytes(), con);

	}

	/***
	 * 数据操作语言
	 * 
	 * @param con
	 * @param commandComplete
	 * @param response
	 */
	private void doProcessCommandComplete(PostgreSQLBackendConnection con, CommandComplete commandComplete, SelectResponse response) {
		if(commandComplete.isSelectComplete()){
			if(response == null){
				throw new RuntimeException("the select proess err ,the SelectResponse is empty");
			}
			doProcessBusinessQuery(con, response ,commandComplete);
		}else{
			OkPacket okPck = new OkPacket();
			okPck.affectedRows = 0;
			okPck.insertId = 0;
			okPck.packetId = ++packetId;
			okPck.message = commandComplete.getCommandResponse().trim().getBytes();
			con.getResponseHandler().okResponse(okPck.writeToBytes(), con);
		}
	}



	/*****
	 * 处理简单查询
	 * 
	 * @param con
	 * @param packets
	 */
	private void doProcessBusinessQuery(PostgreSQLBackendConnection con, SelectResponse response,CommandComplete commandComplete) {
		RowDescription rowHd = response.getDescription();
		List<FieldPacket> fieldPks = PgPacketApaterUtils.rowDescConvertFieldPacket(rowHd);
		List<RowDataPacket> rowDatas = new ArrayList<>();
		for (DataRow dataRow : response.getDataRows()) {
			rowDatas.add(PgPacketApaterUtils.rowDataConvertRowDataPacket(dataRow));
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
		try {
			List<PostgreSQLPacket> packets = PacketUtils.parsePacket(buf, 0, readedLength);
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
	
	static class SelectResponse{
		private RowDescription description;
		
		private List<DataRow> dataRows = new ArrayList<>();

		public List<DataRow> getDataRows() {
			return dataRows;
		}

		public void addDataRow(DataRow packet) {
			this.dataRows.add(packet);
		}

		public void setDataRows(List<DataRow> dataRows) {
			this.dataRows = dataRows;
		}

		public RowDescription getDescription() {
			return description;
		}
		
		
		public SelectResponse(RowDescription description) {
			this.description = description;
		}
		
		
	}

}
