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
package io.mycat.backend.heartbeat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.config.model.DataHostConfig;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import io.mycat.util.TimeUtil;

/**
 * @author mycat
 */
public class MySQLDetector implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
	
	private MySQLHeartbeat heartbeat;
	
	private long heartbeatTimeout;
	private final AtomicBoolean isQuit;
	private volatile long lastSendQryTime;
	private volatile long lasstReveivedQryTime;
	private volatile SQLJob sqlJob;
	
	private static final String[] MYSQL_SLAVE_STAUTS_COLMS = new String[] {
			"Seconds_Behind_Master", 
			"Slave_IO_Running", 
			"Slave_SQL_Running",
			"Slave_IO_State",
			"Master_Host",
			"Master_User",
			"Master_Port", 
			"Connect_Retry",
			"Last_IO_Error"};

	private static final String[] MYSQL_CLUSTER_STAUTS_COLMS = new String[] {
			"Variable_name",
			"Value"};
	
	public MySQLDetector(MySQLHeartbeat heartbeat) {
		this.heartbeat = heartbeat;
		this.isQuit = new AtomicBoolean(false);
	}

	public MySQLHeartbeat getHeartbeat() {
		return heartbeat;
	}

	public long getHeartbeatTimeout() {
		return heartbeatTimeout;
	}

	public void setHeartbeatTimeout(long heartbeatTimeout) {
		this.heartbeatTimeout = heartbeatTimeout;
	}

	public boolean isHeartbeatTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastSendQryTime,
				lasstReveivedQryTime) + heartbeatTimeout;
	}

	public long getLastSendQryTime() {
		return lastSendQryTime;
	}

	public long getLasstReveivedQryTime() {
		return lasstReveivedQryTime;
	}

	public void heartbeat() {
		lastSendQryTime = System.currentTimeMillis();
		MySQLDataSource ds = heartbeat.getSource();
		String databaseName = ds.getDbPool().getSchemas()[0];
		String[] fetchColms={};
		if (heartbeat.getSource().getHostConfig().isShowSlaveSql() ) {
			fetchColms=MYSQL_SLAVE_STAUTS_COLMS;
		}
		if (heartbeat.getSource().getHostConfig().isShowClusterSql() ) {
			fetchColms=MYSQL_CLUSTER_STAUTS_COLMS;
		}
		OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler( fetchColms, this);
		sqlJob = new SQLJob(heartbeat.getHeartbeatSQL(), databaseName, resultHandler, ds);
		sqlJob.run();
	}

	public void quit() {
		if (isQuit.compareAndSet(false, true)) {
			close("heart beat quit");
		}

	}

	public boolean isQuit() {
		return isQuit.get();
	}

	@Override
	public void onResult(SQLQueryResult<Map<String, String>> result) {
		
		if (result.isSuccess()) {
            
			int balance = heartbeat.getSource().getDbPool().getBalance();
            
			PhysicalDatasource source = heartbeat.getSource();
            int switchType = source.getHostConfig().getSwitchType();
            Map<String, String> resultResult = result.getResult();
          
			if ( resultResult!=null&& !resultResult.isEmpty() &&switchType == DataHostConfig.SYN_STATUS_SWITCH_DS
					&& source.getHostConfig().isShowSlaveSql()) {
				
				String Slave_IO_Running  = resultResult != null ? resultResult.get("Slave_IO_Running") : null;
				String Slave_SQL_Running = resultResult != null ? resultResult.get("Slave_SQL_Running") : null;

				if (Slave_IO_Running != null 
						&& Slave_IO_Running.equals(Slave_SQL_Running) 
						&& Slave_SQL_Running.equals("Yes")) {
					
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
					String Seconds_Behind_Master = resultResult.get( "Seconds_Behind_Master");					
					if (null != Seconds_Behind_Master && !"".equals(Seconds_Behind_Master)) {
						
						int Behind_Master = Integer.parseInt(Seconds_Behind_Master);
						if ( Behind_Master >  source.getHostConfig().getSlaveThreshold() ) {
							MySQLHeartbeat.LOGGER.warn("found MySQL master/slave Replication delay !!! "
									+ heartbeat.getSource().getConfig() + ", binlog sync time delay: " + Behind_Master + "s" );
						}						
						heartbeat.setSlaveBehindMaster( Behind_Master );
					}
					
				} else if( source.isSalveOrRead() ) {					
					//String Last_IO_Error = resultResult != null ? resultResult.get("Last_IO_Error") : null;					
					MySQLHeartbeat.LOGGER.warn("found MySQL master/slave Replication err !!! " 
								+ heartbeat.getSource().getConfig() + ", " + resultResult);
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
				}

				heartbeat.getAsynRecorder().set(resultResult, switchType);
				heartbeat.setResult(MySQLHeartbeat.OK_STATUS, this,  null);
				
            } else if ( resultResult!=null&& !resultResult.isEmpty() && switchType==DataHostConfig.CLUSTER_STATUS_SWITCH_DS
            		&& source.getHostConfig().isShowClusterSql() ) {
            	
				//String Variable_name = resultResult != null ? resultResult.get("Variable_name") : null;
				String wsrep_cluster_status = resultResult != null ? resultResult.get("wsrep_cluster_status") : null;// Primary
				String wsrep_connected = resultResult != null ? resultResult.get("wsrep_connected") : null;// ON
				String wsrep_ready = resultResult != null ? resultResult.get("wsrep_ready") : null;// ON
				
				if ("ON".equals(wsrep_connected) 
						&& "ON".equals(wsrep_ready)
						&& "Primary".equals(wsrep_cluster_status)) {
					
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
					heartbeat.setResult(MySQLHeartbeat.OK_STATUS, this, null);
					
				} else {					
					MySQLHeartbeat.LOGGER.warn("found MySQL  cluster status err !!! " 
							+ heartbeat.getSource().getConfig() 
							+ " wsrep_cluster_status: "+ wsrep_cluster_status  
							+ " wsrep_connected: "+ wsrep_connected
							+ " wsrep_ready: "+ wsrep_ready
					);
					
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
					heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS, this,  null);
				}				
				heartbeat.getAsynRecorder().set(resultResult, switchType);
    			
			} else {				
    			heartbeat.setResult(MySQLHeartbeat.OK_STATUS, this,  null);
    		}
			//监测数据库同步状态，在 switchType=-1或者1的情况下，也需要收集主从同步状态
			heartbeat.getAsynRecorder().set(resultResult, switchType);
            
		} else {
			heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS, this,  null);
		}
		
		lasstReveivedQryTime = System.currentTimeMillis();
		heartbeat.getRecorder().set((lasstReveivedQryTime - lastSendQryTime));
	}

	public void close(String msg) {
		SQLJob curJob = sqlJob;
		if (curJob != null && !curJob.isFinished()) {
			curJob.teminate(msg);
			sqlJob = null;
		}
	}
}
