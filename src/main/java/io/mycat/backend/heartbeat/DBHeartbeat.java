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

import io.mycat.statistic.DataSourceSyncRecorder;
import io.mycat.statistic.HeartbeatRecorder;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 数据库心跳
 */
public abstract class DBHeartbeat {
	/**
	 * 数据库同步错误
	 */
	public static final int DB_SYN_ERROR = -1;
	/**
	 * 数据库同步正常
	 */
	public static final int DB_SYN_NORMAL = 1;

	/**
	 * 正常状态
	 */
	public static final int OK_STATUS = 1;
	/**
	 * 错误状态
	 */
	public static final int ERROR_STATUS = -1;
	/**
	 * 超时状态
	 */
	public static final int TIMEOUT_STATUS = -2;
	/**
	 * 初始化状态
	 */
	public static final int INIT_STATUS = 0;

	// 默认心跳时间
	private static final long DEFAULT_HEARTBEAT_TIMEOUT = 30 * 1000L;
	// 默认心跳重试次数
	private static final int DEFAULT_HEARTBEAT_RETRY = 10;
	// 心跳配置
	// 心跳超时时间
	protected long heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;
	// 检查连接发生异常到切换，重试次数
	protected int heartbeatRetry = DEFAULT_HEARTBEAT_RETRY;
	// 静态心跳语句
	protected String heartbeatSQL;
	protected final AtomicBoolean isStop = new AtomicBoolean(true);
	protected final AtomicBoolean isChecking = new AtomicBoolean(false);
	// 错误次数
	protected AtomicInteger errorCount = new AtomicInteger(0);
	// 状态
	protected volatile int status;
	// 心跳记录
	protected final HeartbeatRecorder recorder = new HeartbeatRecorder();
	// 数据源同步记录
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

}