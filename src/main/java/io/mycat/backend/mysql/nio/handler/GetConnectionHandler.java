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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;

/**
 * wuzh
 * 
 * @author mycat
 * 
 */
public class GetConnectionHandler implements ResponseHandler {
	private final CopyOnWriteArrayList<BackendConnection> successCons;
	private static final Logger logger = LoggerFactory
			.getLogger(GetConnectionHandler.class);
	private final AtomicInteger finishedCount = new AtomicInteger(0);
	private final int total;

	public GetConnectionHandler(
			CopyOnWriteArrayList<BackendConnection> connsToStore,
			int totalNumber) {
		super();
		this.successCons = connsToStore;
		this.total = totalNumber;
	}

	public String getStatusInfo()
	{
		return "finished "+ finishedCount.get()+" success "+successCons.size()+" target count:"+this.total;
	}
	public boolean finished() {
		return finishedCount.get() >= total;
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		successCons.add(conn);
		finishedCount.addAndGet(1);
		logger.info("connected successfuly " + conn);
        conn.release();
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		finishedCount.addAndGet(1);
		logger.warn("connect error " + conn+ e);
        conn.release();
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		logger.warn("caught error resp: " + conn + " " + new String(err));
        conn.release();
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		logger.info("received ok resp: " + conn + " " + new String(ok));

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