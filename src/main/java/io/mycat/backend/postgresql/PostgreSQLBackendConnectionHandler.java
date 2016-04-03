package io.mycat.backend.postgresql;

import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.utils.PacketUtils;
import io.mycat.net.handler.BackendAsyncHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.alibaba.fastjson.JSON;

public class PostgreSQLBackendConnectionHandler extends BackendAsyncHandler {
	private static final int RESULT_STATUS_INIT = 0;
	private static final int RESULT_STATUS_HEADER = 1;
	private static final int RESULT_STATUS_FIELD_EOF = 2;
	private volatile int resultStatus;

	/**
	 * life cycle: one SQL execution
	 */
	private transient volatile ResponseHandler responseHandler;

	/*****
	 * 每个后台响应有唯一的连接
	 */
	private final PostgreSQLBackendConnection source;

	public PostgreSQLBackendConnectionHandler(PostgreSQLBackendConnection source) {
		this.source = source;
	}

	@Override
	public void handle(byte[] data) {
		offerData(data, source.getProcessor().getExecutor());
	}

	@Override
	protected void offerDataError() {
		resultStatus = RESULT_STATUS_INIT;
		throw new RuntimeException("offer data error!");
	}

	@Override
	protected void handleData(byte[] data) {
		try {
			List<PostgreSQLPacket> pgs = PacketUtils.parsePacket(ByteBuffer.wrap(data), 0, data.length);
			System.out.println(JSON.toJSONString(pgs));
		} catch (IOException e) {

		}
	}
}