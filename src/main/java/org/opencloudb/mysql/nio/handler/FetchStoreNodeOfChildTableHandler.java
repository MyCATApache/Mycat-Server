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
package org.opencloudb.mysql.nio.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.ConnectionMeta;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.CachePool;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;

/**
 * company where id=(select company_id from customer where id=3); the one which
 * return data (id) is the datanode to store child table's records
 * 
 * @author wuzhih
 * 
 */
public class FetchStoreNodeOfChildTableHandler implements ResponseHandler {
	private static final Logger LOGGER = Logger
			.getLogger(FetchStoreNodeOfChildTableHandler.class);
	private String sql;
	private volatile String result;
	private volatile String dataNode;
	private AtomicInteger finished = new AtomicInteger(0);
	protected final ReentrantLock lock = new ReentrantLock();

	public String execute(String schema, String sql, ArrayList<String> dataNodes) {
		String key = schema + ":" + sql;
		CachePool cache = MycatServer.getInstance().getCacheService()
				.getCachePool("ER_SQL2PARENTID");
		String result = (String) cache.get(key);
		if (result != null) {
			return result;
		}
		this.sql = sql;
		int totalCount = dataNodes.size();
		long startTime = System.currentTimeMillis();
		long endTime = startTime + 5 * 60 * 1000L;
		MycatConfig conf = MycatServer.getInstance().getConfig();

		LOGGER.debug("find child node with sql:" + sql);
		for (String dn : dataNodes) {
			if (dataNode != null) {
				return dataNode;
			}
			PhysicalDBNode mysqlDN = conf.getDataNodes().get(dn);
			try {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("execute in datanode " + dn);
				}
				mysqlDN.getConnection(mysqlDN.getDatabase(), true,
						new RouteResultsetNode(dn, ServerParse.SELECT, sql),
						this, dn);
			} catch (Exception e) {
				LOGGER.warn("get connection err " + e);
			}
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {

			}
		}

		while (dataNode == null && System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				break;
			}
			if (dataNode != null || finished.get() >= totalCount) {
				break;
			}
		}
		if (dataNode != null) {
			cache.putIfAbsent(key, dataNode);
		}
		return dataNode;

	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		conn.setResponseHandler(this);
		try {
			conn.query(sql);
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		finished.incrementAndGet();
		LOGGER.warn("connectionError " + e);

	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		finished.incrementAndGet();
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
			finished.incrementAndGet();
			conn.release();
		}

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("received rowResponse response," + getColumn(row)
					+ " from  " + conn);
		}
		if (result == null) {
			result = getColumn(row);
			dataNode = (String) conn.getAttachment();
		} else {
			LOGGER.warn("find multi data nodes for child table store, sql is:  "
					+ sql);
		}

	}

	private String getColumn(byte[] row) {
		RowDataPacket rowDataPkg = new RowDataPacket(1);
		rowDataPkg.read(row);
		byte[] columnData = rowDataPkg.fieldValues.get(0);
		return new String(columnData);
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		finished.incrementAndGet();
		conn.release();
	}

	private void executeException(BackendConnection c, Throwable e) {
		finished.incrementAndGet();
		LOGGER.warn("executeException   " + e);
		c.close("exception:" + e);

	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {

		LOGGER.warn("connection closed " + conn + " reason:" + reason);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

	}

}