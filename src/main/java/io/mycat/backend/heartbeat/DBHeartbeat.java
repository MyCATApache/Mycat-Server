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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.mycat.statistic.DataSourceSyncRecorder;
import io.mycat.statistic.HeartbeatRecorder;
import io.mycat.util.LogUtil;

public abstract class DBHeartbeat {
	public static final int DB_SYN_ERROR = -1;
	public static final int DB_SYN_NORMAL = 1;

	public static final int OK_STATUS = 1;
	public static final int ERROR_STATUS = -1;
	public static final int TIMEOUT_STATUS = -2;
	public static final int INIT_STATUS = 0;
	private static final long DEFAULT_HEARTBEAT_TIMEOUT = 30 * 1000L;
	private static final int DEFAULT_HEARTBEAT_RETRY = 10;
	// heartbeat config
	protected long heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT; // 心跳超时时间
	protected int heartbeatRetry = DEFAULT_HEARTBEAT_RETRY; // 检查连接发生异常到切换，重试次数
	protected String heartbeatSQL;// 静态心跳语句
	protected final AtomicBoolean isStop = new AtomicBoolean(true);
	protected final AtomicBoolean isChecking = new AtomicBoolean(false);
	protected AtomicInteger errorCount = new AtomicInteger(0);
	protected volatile int status;
	protected final HeartbeatRecorder recorder = new HeartbeatRecorder();
	protected final DataSourceSyncRecorder asynRecorder = new DataSourceSyncRecorder();

	private volatile Integer slaveBehindMaster;
	private volatile int dbSynStatus = DB_SYN_NORMAL;

	public Integer getSlaveBehindMaster() {
		return slaveBehindMaster;
	}

	public int getDbSynStatus() {
		return dbSynStatus;
	}

	public void setDbSynStatus(int dbSynStatus) {
		this.dbSynStatus = dbSynStatus;
	}

	public void setSlaveBehindMaster(Integer slaveBehindMaster) {
		this.slaveBehindMaster = slaveBehindMaster;
	}

	public int getStatus() {
		return status;
	}

	public boolean isChecking() {
		return isChecking.get();
	}

	public abstract void start();

	public abstract void stop();

	public boolean isStop() {
		return isStop.get();
	}

	public int getErrorCount() {
		return errorCount.get();
	}

	public HeartbeatRecorder getRecorder() {
		return recorder;
	}

	public abstract String getLastActiveTime();

	public abstract long getTimeout();

	public abstract void heartbeat();

	public long getHeartbeatTimeout() {
		return heartbeatTimeout;
	}

	public void setHeartbeatTimeout(long heartbeatTimeout) {
		this.heartbeatTimeout = heartbeatTimeout;
	}

	public int getHeartbeatRetry() {
		return heartbeatRetry;
	}

	public void setHeartbeatRetry(int heartbeatRetry) {
		this.heartbeatRetry = heartbeatRetry;
	}

	public String getHeartbeatSQL() {
		return heartbeatSQL;
	}

	public void setHeartbeatSQL(String heartbeatSQL) {
		this.heartbeatSQL = heartbeatSQL;
	}

	public boolean isNeedHeartbeat() {
		return heartbeatSQL != null;
	}

	public DataSourceSyncRecorder getAsynRecorder() {
		return this.asynRecorder;
	}
	/*
	 * 
	 * @desc 將心跳的狀態寫入到日誌中
	 * */
	protected void writeStatusMsg(String dataHost, String dataSourceName,int nextstatus) {
		if(status != nextstatus) {
			StringBuilder msg = new StringBuilder("");
			msg.append("[dataHost=").append(dataHost).append(", dataSource=").append(dataSourceName)
			.append(",statue=").append(getMsg(status)).append(" -> ").append(getMsg(nextstatus)).append("]");
			LogUtil.writeDataSourceLog(msg.toString());
		}
	}
	/*
	 * 
	 * @return 獲取對應狀態的字符串狀態
	 * */
	protected String getMsg(int status) {
		switch (status) {
		case DBHeartbeat.INIT_STATUS:
			return "init status";
		case DBHeartbeat.TIMEOUT_STATUS:
			return "timeout status";
		case DBHeartbeat.OK_STATUS:
			return "ok status";
		case DBHeartbeat.ERROR_STATUS:
			return "error status";	
		default:
			return "unknown status";	
		}
	}
}