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
package io.mycat.backend.mysql.nio.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;

public class SimpleLogHandler implements ResponseHandler{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SimpleLogHandler.class);
	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		LOGGER.warn(conn+" connectionError "+e);
		
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		LOGGER.info("connectionAcquired "+conn);
		
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		LOGGER.warn("caught error resp: " + conn + " " + bytesToHex(err));
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		LOGGER.info("okResponse: " + conn + "," + bytesToHex(ok) );
		
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		LOGGER.info("fieldEofResponse : " + conn );
		LOGGER.info("SimpleLogHandler.fieldEofResponse header: " + bytesToHex(header) );
		for(byte[] field : fields) {
			LOGGER.info("SimpleLogHandler.fieldEofResponse fields: " + bytesToHex(field) );
		}
		LOGGER.info("SimpleLogHandler.fieldEofResponse eof: " + bytesToHex(eof) );
		
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		LOGGER.info("rowResponse: " + conn );
		System.out.println("SimpleLogHandler.rowResponse: " + bytesToHex(row) );
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.info("rowEofResponse: " + conn );
		LOGGER.info("SimpleLogHandler.rowEofResponse: " + bytesToHex(eof) );
		
	}

	@Override
	public void writeQueueAvailable() {
		
		
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		
		
	}

	public static String bytesToHex(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bytes) {
			sb.append(String.format("%02x ", b));
		}
		return sb.toString();
	}
}