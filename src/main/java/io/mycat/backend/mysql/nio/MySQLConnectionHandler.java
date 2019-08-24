/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.mysql.nio;

import io.mycat.backend.mysql.ByteUtil;
import io.mycat.backend.mysql.nio.handler.LoadDataResponseHandler;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.net.handler.BackendAsyncHandler;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.RequestFilePacket;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * life cycle: from connection establish to close <br/>
 *
 * @author mycat
 */
public class MySQLConnectionHandler extends BackendAsyncHandler {
	private static final Logger logger = LoggerFactory
			.getLogger(MySQLConnectionHandler.class);
	private static final int RESULT_STATUS_INIT = 0;
	private static final int RESULT_STATUS_HEADER = 1;
	private static final int RESULT_STATUS_FIELD_EOF = 2;

	private final MySQLConnection source;
	private volatile int resultStatus;
	private volatile byte[] header;
	private volatile List<byte[]> fields;
	private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnectionHandler.class);
	/**
	 * life cycle: one SQL execution
	 */
	private volatile ResponseHandler responseHandler;

	public MySQLConnectionHandler(MySQLConnection source) {
		this.source = source;
		this.resultStatus = RESULT_STATUS_INIT;
	}

	public void connectionError(Throwable e) {
		if (responseHandler != null) {
			responseHandler.connectionError(e, source);
		}
		LOGGER.error("connectionError but not handle");
	}

	public MySQLConnection getSource() {
		return source;
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
		switch (resultStatus) {
			case RESULT_STATUS_INIT:
				switch (data[4]) {
					case OkPacket.FIELD_COUNT:
						handleOkPacket(data);
						break;
					case ErrorPacket.FIELD_COUNT:
						handleErrorPacket(data);
						break;
					case RequestFilePacket.FIELD_COUNT:
						handleRequestPacket(data);
						break;
					default:
						resultStatus = RESULT_STATUS_HEADER;
						header = data;
						fields = new ArrayList<byte[]>((int) ByteUtil.readLength(data,
								4));
				}
				break;
			case RESULT_STATUS_HEADER:
				switch (data[4]) {
					case ErrorPacket.FIELD_COUNT:
						resultStatus = RESULT_STATUS_INIT;
						handleErrorPacket(data);
						break;
					case EOFPacket.FIELD_COUNT:
						resultStatus = RESULT_STATUS_FIELD_EOF;
						handleFieldEofPacket(data);
						break;
					default:
						fields.add(data);
				}
				break;
			case RESULT_STATUS_FIELD_EOF:
				switch (data[4]) {
					case ErrorPacket.FIELD_COUNT:
						resultStatus = RESULT_STATUS_INIT;
						handleErrorPacket(data);
						break;
					case EOFPacket.FIELD_COUNT:
						resultStatus = RESULT_STATUS_INIT;
						handleRowEofPacket(data);
						break;
					default:
						handleRowPacket(data);
				}
				break;
			default:
				throw new RuntimeException("unknown status!");
		}
	}

	public void setResponseHandler(ResponseHandler responseHandler) {
		// logger.info("set response handler "+responseHandler);
		// if (this.responseHandler != null && responseHandler != null) {
		// throw new RuntimeException("reset agani!");
		// }
		this.responseHandler = responseHandler;
	}

	/**
	 * OK数据包处理
	 */
	private void handleOkPacket(byte[] data) {
		ResponseHandler respHand = responseHandler;
		if (respHand != null) {
			respHand.okResponse(data, source);
		}else {
			LOGGER.error("receive OkPacket but not handle");
		}
	}

	/**
	 * ERROR数据包处理
	 */
	private void handleErrorPacket(byte[] data) {
		ResponseHandler respHand = responseHandler;
		if (respHand != null) {
			respHand.errorResponse(data, source);
		} else {
			LOGGER.error("receive ErrorPacket but no handler");
			closeNoHandler();
		}
	}

	/**
	 * load data file 请求文件数据包处理
	 */
	private void handleRequestPacket(byte[] data) {
		ResponseHandler respHand = responseHandler;
		if (respHand != null && respHand instanceof LoadDataResponseHandler) {
			((LoadDataResponseHandler) respHand).requestDataResponse(data,
					source);
		} else {
			LOGGER.error("receive RequestPacket but no handler");
			closeNoHandler();
		}
	}

	/**
	 * 字段数据包结束处理
	 */
	private void handleFieldEofPacket(byte[] data) {
		ResponseHandler respHand = responseHandler;
		if (respHand != null) {
			respHand.fieldEofResponse(header, fields, data, source);
		} else {
			LOGGER.error("receive FieldEofPacket but no handler");
			closeNoHandler();
		}
	}

	/**
	 * 行数据包处理
	 */
	private void handleRowPacket(byte[] data) {
		ResponseHandler respHand = responseHandler;
		if (respHand != null) {
			respHand.rowResponse(data, source);
		} else {
			LOGGER.error("receive RowPacket but no handler");
			closeNoHandler();

		}
	}

	private void closeNoHandler() {
		if (!source.isClosedOrQuit()) {
			source.close("no handler");
			logger.warn("no handler bind in this con " + this + " client:"
					+ source);
		}
	}

	/**
	 * 行数据包结束处理
	 */
	private void handleRowEofPacket(byte[] data) {
		if (responseHandler != null) {
			responseHandler.rowEofResponse(data, source);
		} else {
			LOGGER.error("receive RowEofPacket but no handler");
			closeNoHandler();
		}
	}

}