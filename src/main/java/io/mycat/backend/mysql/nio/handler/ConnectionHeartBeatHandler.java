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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.net.mysql.ErrorPacket;

/**
 * heartbeat check for mysql connections
 * 
 * @author wuzhih
 * 
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ConnectionHeartBeatHandler.class);
	protected final ReentrantLock lock = new ReentrantLock();
	private final ConcurrentHashMap<Long, HeartBeatCon> allCons = new ConcurrentHashMap<Long, HeartBeatCon>();

	public void doHeartBeat(BackendConnection conn, String sql) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("do heartbeat for con " + conn);
		}

		try {

			HeartBeatCon hbCon = new HeartBeatCon(conn);
			boolean notExist = (allCons.putIfAbsent(hbCon.conn.getId(), hbCon) == null);
			if (notExist) {
				conn.setResponseHandler(this);
				conn.query(sql);

			}
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	/**
	 * remove timeout connections
	 */
	public void abandTimeOuttedConns() {
		if (allCons.isEmpty()) {
			return;
		}
		Collection<BackendConnection> abandCons = new LinkedList<BackendConnection>();
		long curTime = System.currentTimeMillis();
		Iterator<Entry<Long, HeartBeatCon>> itors = allCons.entrySet()
				.iterator();
		while (itors.hasNext()) {
			HeartBeatCon hbCon = itors.next().getValue();
			if (hbCon.timeOutTimestamp < curTime) {
				abandCons.add(hbCon.conn);
				itors.remove();
			}
		}

		if (!abandCons.isEmpty()) {
			for (BackendConnection con : abandCons) {
				try {
					// if(con.isBorrowed())
					con.close("heartbeat timeout ");
				} catch (Exception e) {
					LOGGER.warn("close err:" + e);
				}
			}
		}

	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		// not called
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		// not called

	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		removeFinished(conn);
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		LOGGER.warn("errorResponse " + err.errno + " "
				+ new String(err.message));
		conn.release();

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			removeFinished(conn);
			conn.release();
		}

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		removeFinished(conn);
		conn.release();
	}

	private void executeException(BackendConnection c, Throwable e) {
		removeFinished(c);
		LOGGER.warn("executeException   ", e);
		c.close("heatbeat exception:" + e);

	}

	private void removeFinished(BackendConnection con) {
		Long id = ((BackendConnection) con).getId();
		this.allCons.remove(id);
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		removeFinished(conn);
		LOGGER.warn("connection closed " + conn + " reason:" + reason);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("received field eof  from " + conn);
		}
	}

}

class HeartBeatCon {
	public final long timeOutTimestamp;
	public final BackendConnection conn;

	public HeartBeatCon(BackendConnection conn) {
		super();
		this.timeOutTimestamp = System.currentTimeMillis() + 20 * 1000L;
		this.conn = conn;
	}

}