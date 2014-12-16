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
package org.opencloudb.heartbeat;

import org.opencloudb.exception.HeartbeatException;
import org.opencloudb.net.handler.BackendAsyncHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.OkPacket;

/**
 * @author mycat
 */
public class MySQLDetectorHandler extends BackendAsyncHandler {
	private static final int RESULT_STATUS_INIT = 0;
	private static final int RESULT_STATUS_HEADER = 1;
	private static final int RESULT_STATUS_FIELD_EOF = 2;

	private final MySQLDetector source;
	private volatile int resultStatus;

	public MySQLDetectorHandler(MySQLDetector source) {
		this.source = source;
		this.resultStatus = RESULT_STATUS_INIT;
	}

	@Override
	public void handle(byte[] data) {
		offerData(data, source.getProcessor().getExecutor());
	}

	@Override
	protected void offerDataError() {
		dataQueue.clear();
		resultStatus = RESULT_STATUS_INIT;
		throw new HeartbeatException("offer data error!");
	}

	@Override
	protected void handleData(byte[] data) {
		switch (resultStatus) {
		case RESULT_STATUS_INIT:
			switch (data[4]) {
			case OkPacket.FIELD_COUNT:
				handleOkPacket();
				break;
			case ErrorPacket.FIELD_COUNT:
				handleErrorPacket(data);
				break;
			default:
				resultStatus = RESULT_STATUS_HEADER;
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
				break;
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
				handleRowEofPacket();
				break;
			}
			break;
		default:
			throw new HeartbeatException("unknown status!");
		}
	}

	/**
	 * OK数据包处理
	 */
	private void handleOkPacket() {
		source.getHeartbeat().setResult(MySQLHeartbeat.OK_STATUS, source,
				false, null);
	}

	/**
	 * ERROR数据包处理
	 */
	private void handleErrorPacket(byte[] data) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		throw new HeartbeatException(new String(err.message));
	}

	/**
	 * 行数据包结束处理
	 */
	private void handleRowEofPacket() {
		source.getHeartbeat().setResult(MySQLHeartbeat.OK_STATUS, source,
				false, null);
	}

}