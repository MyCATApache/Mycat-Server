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

/**
 * @author mycat
 */
public class RollbackReleaseHandler implements ResponseHandler {
	private static final Logger logger = LoggerFactory
			.getLogger(RollbackReleaseHandler.class);

	public RollbackReleaseHandler() {
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		logger.error("unexpected invocation: connectionAcquired from rollback-release");
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {

	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		conn.quit();
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		logger.debug("autocomit is false,but no commit or rollback ,so mycat rollbacked backend conn "+conn);
		conn.release();
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {

	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {

	}

}