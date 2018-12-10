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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.cache.CachePool;
import io.mycat.config.MycatConfig;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;

/**
 * company where id=(select company_id from customer where id=3); the one which
 * return data (id) is the datanode to store child table's records
 * 
 * @author wuzhih
 * 
 */
public class FetchStoreNodeOfChildTableHandler implements ResponseHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(FetchStoreNodeOfChildTableHandler.class);
	private String sql;
	private volatile String result;
	private volatile String dataNode;
	private AtomicInteger finished = new AtomicInteger(0);
	protected final ReentrantLock lock = new ReentrantLock();
	private volatile ServerConnection sc; 
	public String execute(String schema, String sql, List<String> dataNodes, ServerConnection sc) {
		
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
		this.sc = sc;
		LOGGER.debug("find child node with sql:" + sql);
		for (String dn : dataNodes) {
			if (dataNode != null) {
				return dataNode;
			}
			PhysicalDBNode mysqlDN = conf.getDataNodes().get(dn);
			try {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(sc + "execute in datanode " + dn);
				}
				RouteResultsetNode node = new RouteResultsetNode(dn, ServerParse.SELECT, sql);
				node.setRunOnSlave(false);	// 获取 子表节点，最好走master为好

				/*
				 * fix #1370 默认应该先从已经持有的连接中取连接, 否则可能因为事务隔离性看不到当前事务内更新的数据
				 * Tips: 通过mysqlDN.getConnection获取到的连接不是当前连接
				 *
				 */
				BackendConnection conn = sc.getSession2().getTarget(node);
				if(sc.getSession2().tryExistsCon(conn, node)) {
					_execute(conn, node, sc);
				} else {
					mysqlDN.getConnection(mysqlDN.getDatabase(), sc.isAutocommit(), node, this, node);
				}
			} catch (Exception e) {
				LOGGER.warn("get connection err " + e);
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
		if(System.currentTimeMillis() > endTime) {
			LOGGER.error("timeout when executing fetch sql  " + sql);

		}
		return dataNode;
		
	}

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
				RouteResultsetNode node = new RouteResultsetNode(dn, ServerParse.SELECT, sql);
				node.setRunOnSlave(false);	// 获取 子表节点，最好走master为好

				mysqlDN.getConnection(mysqlDN.getDatabase(), true, node, this, node);
				 
//				mysqlDN.getConnection(mysqlDN.getDatabase(), true,
//						new RouteResultsetNode(dn, ServerParse.SELECT, sql),
//						this, dn);
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
	
	private void _execute(BackendConnection conn, RouteResultsetNode node, ServerConnection sc) {
		conn.setResponseHandler(this);
		try {
			conn.execute(node, sc, sc.isAutocommit());
		} catch (IOException e) {
			connectionError(e, conn);
		}
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
		releaseConnection(conn);
		LOGGER.warn(this.sc + " connection release " + conn + " errorResponse" );

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			finished.incrementAndGet();
			//conn.release();
			releaseConnection(conn);
		}

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(this.sc + "received rowResponse response," + getColumn(row)
			+ " from  " + conn);
		}
		if (result == null) {
			result = getColumn(row);
			dataNode = ((RouteResultsetNode) conn.getAttachment()).getName();
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
		//conn.release();
		releaseConnection(conn);
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
		finished.incrementAndGet();
		LOGGER.warn("connection closed " + conn + " reason:" + reason);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

	}
	private void releaseConnection(BackendConnection conn) {
		if(this.sc  != null ) {
			Map<RouteResultsetNode, BackendConnection> target = sc.getSession2().getTargetMap();
			for(BackendConnection backConn :target.values()) {
				if(backConn != null && backConn.equals(conn)) {
					return ;
				}
			}
//			if(sc.getSession2().tryExistsCon(conn, node)) {
//				_execute(conn, node, sc);
//			} else {
//				mysqlDN.getConnection(mysqlDN.getDatabase(), sc.isAutocommit(), node, this, node);
//			}
		}
		conn.release();
	}
}