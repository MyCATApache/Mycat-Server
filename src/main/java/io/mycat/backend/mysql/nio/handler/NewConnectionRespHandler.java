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

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;

public class NewConnectionRespHandler implements ResponseHandler{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(NewConnectionRespHandler.class);
	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		LOGGER.warn(conn+" connectionError "+e);
		
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		//
		LOGGER.info("connectionAcquired "+conn);
		
		conn.release(); //  NewConnectionRespHandler 因为这个是由于空闲连接数低于配置，需要新建连接，但再新建连接的时候，
		
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		LOGGER.warn("caught error resp: " + conn + " " + new String(err));
		conn.release();
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		LOGGER.info("okResponse: " + conn );
		conn.release();
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		LOGGER.info("fieldEofResponse: " + conn );
		
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		LOGGER.info("rowResponse: " + conn );
		
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.info("rowEofResponse: " + conn );
		conn.release();
	}

	@Override
	public void writeQueueAvailable() {
		
		
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		
		
	}

}