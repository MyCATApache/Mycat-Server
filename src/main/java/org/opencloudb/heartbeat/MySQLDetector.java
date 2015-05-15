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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.mysql.nio.MySQLDataSource;
import org.opencloudb.sqlengine.OneRawSQLQueryResultHandler;
import org.opencloudb.sqlengine.SQLJob;
import org.opencloudb.sqlengine.SQLQueryResult;
import org.opencloudb.sqlengine.SQLQueryResultListener;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public class MySQLDetector implements
		SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
	private MySQLHeartbeat heartbeat;
	private long heartbeatTimeout;
	private final AtomicBoolean isQuit;
	private volatile long lastSendQryTime;
	private volatile long lasstReveivedQryTime;
	private volatile SQLJob sqlJob;
	private static final String[] MYSQL_SLAVE_STAUTS_COLMS = new String[] {
			"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running" };

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
		if (heartbeat.getSource().getHostConfig().getSwitchType() == DataHostConfig.SYN_STATUS_SWITCH_DS) {
			fetchColms=MYSQL_SLAVE_STAUTS_COLMS;
		}
		OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(
				fetchColms, this);
		sqlJob = new SQLJob(heartbeat.getHeartbeatSQL(), databaseName,
				resultHandler, ds);
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
	public void onRestult(SQLQueryResult<Map<String, String>> result) {
		if (result.isSuccess()) {
			if (heartbeat.getSource().getHostConfig().getSwitchType() == DataHostConfig.SYN_STATUS_SWITCH_DS) {
				String Slave_IO_Running = result.getResult().get(
						"Slave_IO_Running");
				String Slave_SQL_Running = result.getResult().get(
						"Slave_SQL_Running");
				if (Slave_IO_Running != null
						&& Slave_IO_Running.equals(Slave_SQL_Running)
						&& Slave_SQL_Running.equals("Yes")) {
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
					String Seconds_Behind_Master = result.getResult().get(
							"Seconds_Behind_Master");
					if (null != Seconds_Behind_Master
							&& !"".equals(Seconds_Behind_Master)) {
						heartbeat.setSlaveBehindMaster(Integer
								.valueOf(Seconds_Behind_Master));
					}
				} else {
					MySQLHeartbeat.LOGGER
							.warn("found MySQL master/slave Replication err !!! "
									+ heartbeat.getSource().getConfig());
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
				}

			}
			heartbeat.setResult(MySQLHeartbeat.OK_STATUS, this,  null);
		} else {
			heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS, this,  null);
		}
		lasstReveivedQryTime = System.currentTimeMillis();
	}

	public void close(String msg) {
		SQLJob curJob = sqlJob;
		if (curJob != null && !curJob.isFinished()) {
			curJob.teminate(msg);
			sqlJob = null;
		}
	}

}