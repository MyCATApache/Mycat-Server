package io.mycat.backend.nio;

import io.mycat.MycatServer;
import io.mycat.net.Connection;
import io.mycat.net.ConnectionException;
import io.mycat.net.NIOHandler;
import io.mycat.server.Capabilities;
import io.mycat.server.executors.LoadDataResponseHandler;
import io.mycat.server.executors.ResponseHandler;
import io.mycat.server.packet.*;
import io.mycat.server.packet.util.ByteUtil;
import io.mycat.server.packet.util.CharsetUtil;
import io.mycat.server.packet.util.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class MySQLBackendConnectionHandler implements
		NIOHandler<MySQLBackendConnection> {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(MySQLBackendConnectionHandler.class);
	private static final int RESULT_STATUS_INIT = 0;
	private static final int RESULT_STATUS_HEADER = 1;
	private static final int RESULT_STATUS_FIELD_EOF = 2;

	@Override
	public void onConnected(MySQLBackendConnection con) throws IOException {
		
		//con.asynRead();
	}

	@Override
	public void handle(MySQLBackendConnection con, ByteBuffer buf, int start,
			int readedLength) {
		switch (con.getState()) {
		case connecting: {
			doConnecting(con, buf, start, readedLength);
			return;
		}
		case connected: {
			try {
				doHandleBusinessMsg(con, buf, start, readedLength);
			} catch (Exception e) {
				LOGGER.warn("caught err of con "+con, e);
			}
			return;
		}

		default:
			LOGGER.warn("not handled connecton state  err " + con.getState()
					+ " for con " + con);
			break;

		}

	}

	@Override
	public void onConnectFailed(MySQLBackendConnection source, Throwable e) {
		ResponseHandler respHand = source.getRespHandler();
		if (respHand != null) {
			respHand.connectionError(e, source);
		}

	}

	private void handleLogin(MySQLBackendConnection source, byte[] data) {
		try {
			switch (data[4]) {
			case OkPacket.FIELD_COUNT:
				HandshakePacket packet = source.getHandshake();
				if (packet == null) {
					processHandShakePacket(source, data);
					// 发送认证数据包
					source.authenticate();
					break;
				}
				// 处理认证结果
				source.setAuthenticated(true);
				source.setState(Connection.State.connected);
				boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & packet.serverCapabilities);
				boolean usingCompress = MycatServer.getInstance().getConfig()
						.getSystem().getUseCompression() == 1;
				
				if (clientCompress && usingCompress) {
					source.setSupportCompress(true);
				}
				
				if (source.getRespHandler() != null) {
					source.getRespHandler().connectionAcquired(source);
				}
			
				break;
			case ErrorPacket.FIELD_COUNT:
				ErrorPacket err = new ErrorPacket();
				err.read(data);
				String errMsg = new String(err.message);
				LOGGER.warn("can't connect to mysql server ,errmsg:" + errMsg
						+ " " + source);
				// source.close(errMsg);
				throw new ConnectionException(err.errno, errMsg);

			case EOFPacket.FIELD_COUNT:
				auth323(source, data[3]);
				break;
			default:
				packet = source.getHandshake();
				if (packet == null) {
					processHandShakePacket(source, data);
					// 发送认证数据包
					source.authenticate();
					break;
				} else {
					throw new RuntimeException("Unknown Packet!");
				}

			}

		} catch (RuntimeException e) {
			if (source.getRespHandler() != null) {
				source.getRespHandler().connectionError(e, source);
				return;
			}
			throw e;
		}
	}

	@Override
	public void onClosed(MySQLBackendConnection source, String reason) {
		ResponseHandler respHand = source.getRespHandler();
		if (respHand != null) {
			respHand.connectionClose(source, reason);
		}

	}

	private void doConnecting(MySQLBackendConnection con, ByteBuffer buf,
			int start, int readedLength) {
		byte[] data = new byte[readedLength];
		buf.position(start);
		buf.get(data, 0, readedLength);
		handleLogin(con, data);
	}

	public void doHandleBusinessMsg(final MySQLBackendConnection source,
			final ByteBuffer buf, final int start, final int readedLength) {
		byte[] data = new byte[readedLength];
		buf.position(start);
		buf.get(data, 0, readedLength);
		handleData(source, data);
	}

	public void connectionError(Throwable e) {

	}

	protected void handleData(final MySQLBackendConnection source, byte[] data) {
		ResultStatus resultStatus = source.getSqlResultStatus();
		switch (resultStatus.getResultStatus()) {
		case RESULT_STATUS_INIT:
			switch (data[4]) {
			case OkPacket.FIELD_COUNT:
				handleOkPacket(source, data);
				break;
			case ErrorPacket.FIELD_COUNT:
				handleErrorPacket(source, data);
				break;
			case RequestFilePacket.FIELD_COUNT:
				handleRequestPacket(source, data);
				break;
			default:
				resultStatus.setResultStatus(RESULT_STATUS_HEADER);
				resultStatus.setHeader(data);
				resultStatus.setFields(new ArrayList<byte[]>((int) ByteUtil
						.readLength(data, 4)));
			}
			break;
		case RESULT_STATUS_HEADER:
			switch (data[4]) {
			case ErrorPacket.FIELD_COUNT:
				resultStatus.setResultStatus(RESULT_STATUS_INIT);
				handleErrorPacket(source, data);
				break;
			case EOFPacket.FIELD_COUNT:
				resultStatus.setResultStatus(RESULT_STATUS_FIELD_EOF);
				handleFieldEofPacket(source, data);
				break;
			default:
				resultStatus.getFields().add(data);
			}
			break;
		case RESULT_STATUS_FIELD_EOF:
			switch (data[4]) {
			case ErrorPacket.FIELD_COUNT:
				resultStatus.setResultStatus(RESULT_STATUS_INIT);
				handleErrorPacket(source, data);
				break;
			case EOFPacket.FIELD_COUNT:
				resultStatus.setResultStatus(RESULT_STATUS_INIT);
				handleRowEofPacket(source, data);
				break;
			default:
				handleRowPacket(source, data);
			}
			break;
		default:
			throw new RuntimeException("unknown status!");
		}
	}

	/**
	 * OK数据包处理
	 */
	private void handleOkPacket(MySQLBackendConnection source, byte[] data) {
		ResponseHandler respHand = source.getRespHandler();
		if (respHand != null) {
			respHand.okResponse(data, source);
		}else {
			closeNoHandler(source);
		}
	}

	/**
	 * ERROR数据包处理
	 */
	private void handleErrorPacket(MySQLBackendConnection source, byte[] data) {
		ResponseHandler respHand = source.getRespHandler();
		if (respHand != null) {
			respHand.errorResponse(data, source);
		} else {
			closeNoHandler(source);
		}
	}

	/**
	 * load data file 请求文件数据包处理
	 */
	private void handleRequestPacket(MySQLBackendConnection source, byte[] data) {
		ResponseHandler respHand = source.getRespHandler();
		if (respHand != null && respHand instanceof LoadDataResponseHandler) {
			((LoadDataResponseHandler) respHand).requestDataResponse(data,
					source);
		} else {
			closeNoHandler(source);
		}
	}

	/**
	 * 字段数据包结束处理
	 */
	private void handleFieldEofPacket(final MySQLBackendConnection source,
			byte[] data) {
		ResponseHandler respHand = source.getRespHandler();
		if (respHand != null) {
			respHand.fieldEofResponse(source.getSqlResultStatus().getHeader(),
					source.getSqlResultStatus().getFields(), data, source);
		} else {
			closeNoHandler(source);
		}
	}

	/**
	 * 行数据包处理
	 */
	private void handleRowPacket(final MySQLBackendConnection source,
			byte[] data) {
		ResponseHandler respHand = source.getRespHandler();
		if (respHand != null) {
			respHand.rowResponse(data, source);
		} else {
			closeNoHandler(source);

		}
	}

	private void closeNoHandler(final MySQLBackendConnection source) {
		if (!source.isClosedOrQuit()) {
			source.close("no handler");
			LOGGER.warn("no handler bind in this con " + this + " client:"
					+ source);
		}
	}

	/**
	 * 行数据包结束处理
	 */
	private void handleRowEofPacket(final MySQLBackendConnection source,
			byte[] data) {
		ResponseHandler responseHandler = source.getRespHandler();
		if (responseHandler != null) {
			responseHandler.rowEofResponse(data, source);
		} else {
			closeNoHandler(source);
		}
	}

	private void processHandShakePacket(MySQLBackendConnection source,
			byte[] data) {
		// 设置握手数据包
		HandshakePacket packet = new HandshakePacket();
		packet.read(data);
		source.setHandshake(packet);
		source.setThreadId(packet.threadId);

		// 设置字符集编码
		int charsetIndex = (packet.serverCharsetIndex & 0xff);
		String charset = CharsetUtil.getCharset(charsetIndex);
		if (charset != null) {
			source.setCharset(charset);
		} else {
			LOGGER.warn("Unknown charsetIndex:" + charsetIndex);
			throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
		}
	}

	private void auth323(MySQLBackendConnection source, byte packetId) {
		// 发送323响应认证数据包
		Reply323Packet r323 = new Reply323Packet();
		r323.packetId = ++packetId;
		String pass = source.getPassword();
		if (pass != null && pass.length() > 0) {
			byte[] seed = source.getHandshake().seed;
			r323.seed = SecurityUtil.scramble323(pass, new String(seed))
					.getBytes();
		}
		r323.write(source);
	}

}
