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
package io.mycat.backend.datasource;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.route.RouteResultsetNode;

public class PhysicalDBNode {
	protected static final Logger LOGGER = LoggerFactory
			.getLogger(PhysicalDBNode.class);

	protected final String name;
	protected final String database;
	protected final PhysicalDBPool dbPool;

	public PhysicalDBNode(String hostName, String database,
			PhysicalDBPool dbPool) {
		this.name = hostName;
		this.database = database;
		this.dbPool = dbPool;
	}

	public String getName() {
		return name;
	}

	public PhysicalDBPool getDbPool() {
		return dbPool;
	}

	public String getDatabase() {
		return database;
	}

	/**
	 * get connection from the same datasource
	 * 
	 * @param exitsCon
	 * @throws Exception
	 */
	public void getConnectionFromSameSource(String schema,boolean autocommit,
			BackendConnection exitsCon, ResponseHandler handler,
			Object attachment) throws Exception {

		PhysicalDatasource ds = this.dbPool.findDatasouce(exitsCon);
		if (ds == null) {
			throw new RuntimeException(
					"can't find exits connection,maybe fininshed " + exitsCon);
		} else {
			ds.getConnection(schema,autocommit, handler, attachment);
		}

	}

	private void checkRequest(String schema){
		if (schema != null
				&& !schema.equals(this.database)) {
			throw new RuntimeException(
					"invalid param ,connection request db is :"
							+ schema + " and datanode db is "
							+ this.database);
		}
		if (!dbPool.isInitSuccess()) {
			dbPool.init(dbPool.activedIndex);
		}
	}
	
	public void getConnection(String schema,boolean autoCommit, RouteResultsetNode rrs,
							ResponseHandler handler, Object attachment) throws Exception {
		checkRequest(schema);
		if (dbPool.isInitSuccess()) {
			LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
			if(rrs.getRunOnSlave() != null){		// 带有 /*db_type=master/slave*/ 注解
				// 强制走 slave
				if(rrs.getRunOnSlave()){			
					LOGGER.debug("rrs.isHasBlanceFlag() " + rrs.isHasBlanceFlag());
					if (rrs.isHasBlanceFlag()) {		// 带有 /*balance*/ 注解(目前好像只支持一个注解...)
						dbPool.getReadBanlanceCon(schema,autoCommit,handler, attachment, this.database);
					}else{	// 没有 /*balance*/ 注解
						LOGGER.debug("rrs.isHasBlanceFlag()" + rrs.isHasBlanceFlag());
						if(!dbPool.getReadCon(schema, autoCommit, handler, attachment, this.database)){
							LOGGER.warn("Do not have slave connection to use, use master connection instead.");
							PhysicalDatasource writeSource=dbPool.getSource();
							//记录写节点写负载值
							writeSource.setWriteCount();
							writeSource.getConnection(schema,
									autoCommit, handler, attachment);
							rrs.setRunOnSlave(false);
							rrs.setCanRunInReadDB(false);
						}
					}
				}else{	// 强制走 master
					// 默认获得的是 writeSource，也就是 走master
					LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
					PhysicalDatasource writeSource=dbPool.getSource();
					//记录写节点写负载值
					writeSource.setReadCount();
					writeSource.getConnection(schema, autoCommit,
							handler, attachment);
					rrs.setCanRunInReadDB(false);
				}
			}else{	// 没有  /*db_type=master/slave*/ 注解，按照原来的处理方式
				LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());	// null
				if (rrs.canRunnINReadDB(autoCommit)) {
					dbPool.getRWBanlanceCon(schema,autoCommit, handler, attachment, this.database);
				} else {
					PhysicalDatasource writeSource =dbPool.getSource();
					//记录写节点写负载值
					writeSource.setWriteCount();
					writeSource.getConnection(schema, autoCommit,
							handler, attachment);
				}
			}
		
		} else {
			throw new IllegalArgumentException("Invalid DataSource:" + dbPool.getActivedIndex());
			}
		}

//	public void getConnection(String schema,boolean autoCommit, RouteResultsetNode rrs,
//			ResponseHandler handler, Object attachment) throws Exception {
//		checkRequest(schema);
//		if (dbPool.isInitSuccess()) {
//			if (rrs.canRunnINReadDB(autoCommit)) {
//				dbPool.getRWBanlanceCon(schema,autoCommit, handler, attachment,
//						this.database);
//			} else {
//				dbPool.getSource().getConnection(schema,autoCommit, handler, attachment);
//			}
//
//		} else {
//			throw new IllegalArgumentException("Invalid DataSource:"
//					+ dbPool.getActivedIndex());
//		}
//	}
}