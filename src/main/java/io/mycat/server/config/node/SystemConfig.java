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
package io.mycat.server.config.node;

import io.mycat.server.Isolations;

import java.io.File;
import java.io.IOException;

/**
 * 系统基础配置项
 * 
 * @author mycat
 */
public final class SystemConfig {

	public static final String SYS_HOME = "MYCAT_HOME";
	private static final int DEFAULT_PORT = 8066;
	private static final int DEFAULT_MANAGER_PORT = 9066;
	private static final String DEFAULT_CHARSET = "utf8";

	private static final String DEFAULT_SQL_PARSER = "fdbparser";// druidparser
	private static final int DEFAULT_BUFFER_CHUNK_SIZE = 4096;
	private int processorBufferLocalPercent;
	private static final int DEFAULT_PROCESSORS = Runtime.getRuntime()
			.availableProcessors();
	private int frontSocketSoRcvbuf = 1024 * 1024;
	private int frontSocketSoSndbuf = 4 * 1024 * 1024;
	private int backSocketSoRcvbuf = 4 * 1024 * 1024;// mysql 5.6
														// net_buffer_length
														// defaut 4M
	private int backSocketSoSndbuf = 1024 * 1024;
	private int frontSocketNoDelay = 1; // 0=false
	private int backSocketNoDelay = 1; // 1=true
	public static final int DEFAULT_POOL_SIZE = 128;// 保持后端数据通道的默认最大值
	public static final long DEFAULT_IDLE_TIMEOUT = 30 * 60 * 1000L;
	private static final long DEFAULT_PROCESSOR_CHECK_PERIOD = 1 * 1000L;
	private static final long DEFAULT_DATANODE_IDLE_CHECK_PERIOD = 5 * 60 * 1000L;
	private static final long DEFAULT_DATANODE_HEARTBEAT_PERIOD = 10 * 1000L;
	private static final long DEFAULT_CLUSTER_HEARTBEAT_PERIOD = 5 * 1000L;
	private static final long DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT = 10 * 1000L;
	private static final int DEFAULT_CLUSTER_HEARTBEAT_RETRY = 10;
	private static final int DEFAULT_MAX_LIMIT = 100;
	private static final String DEFAULT_CLUSTER_HEARTBEAT_USER = "_HEARTBEAT_USER_";
	private static final String DEFAULT_CLUSTER_HEARTBEAT_PASS = "_HEARTBEAT_PASS_";
	private static final int DEFAULT_PARSER_COMMENT_VERSION = 50148;
	private static final int DEFAULT_SQL_RECORD_COUNT = 10;
	// 全局表一致性检测任务，默认24小时调度一次
	private static final long DEFAULT_GLOBAL_TABLE_CHECK_PERIOD = 24 * 60 * 60 * 1000L;
	private boolean globalTableCheckSwitchOn = true;	// 全局表一致性检查开关
	private int maxStringLiteralLength = 65535;
	private int frontWriteQueueSize = 2048;
	private String bindIp = "0.0.0.0";
	private int serverPort;
	private int managerPort;
	private String charset;
	private int processors;
	private int processorExecutor;
	private int timerExecutor;
	private int managerExecutor;
	private long idleTimeout;
	private int catletClassCheckSeconds = 60;
	// sql execute timeout (second)
	private long sqlExecuteTimeout = 300;
	private long processorCheckPeriod;
	private long dataNodeIdleCheckPeriod;
	private long dataNodeHeartbeatPeriod;
	private String clusterHeartbeatUser;
	private String clusterHeartbeatPass;
	private long clusterHeartbeatPeriod;
	private long glableTableCheckPeriod;
	private long clusterHeartbeatTimeout;
	private int clusterHeartbeatRetry;
	
	private int txIsolation;
	private int parserCommentVersion;
	private int sqlRecordCount;
	private long processorBufferPool;
	private int processorBufferChunk;
	private int defaultMaxLimit = DEFAULT_MAX_LIMIT;
	public static final int SEQUENCEHANDLER_LOCALFILE = 0;
	public static final int SEQUENCEHANDLER_MYSQLDB = 1;
	public static final int SEQUENCEHANDLER_LOCAL_TIME = 2;
	private int sequnceHandlerType = SEQUENCEHANDLER_LOCALFILE;
	private String sqlInterceptor = "io.mycat.server.interceptor.impl.DefaultSqlInterceptor";
	private String sqlInterceptorType = "select";
	private String sqlInterceptorFile = System.getProperty("user.dir")
			+ "/logs/sql.txt";
	public static final int MUTINODELIMIT_SMALL_DATA = 0;
	public static final int MUTINODELIMIT_LAR_DATA = 1;
	private int mutiNodeLimitType = MUTINODELIMIT_SMALL_DATA;

	public static final int MUTINODELIMIT_PATCH_SIZE = 100;
	private int mutiNodePatchSize = MUTINODELIMIT_PATCH_SIZE;

	private String defaultSqlParser = DEFAULT_SQL_PARSER;
	private int usingAIO = 0;
	private int packetHeaderSize = 4;
	private int maxPacketSize = 16 * 1024 * 1024;
	private int mycatNodeId = 1;
	private int useCompression = 0;

	public String getDefaultSqlParser() {
		return defaultSqlParser;
	}

	public void setDefaultSqlParser(String defaultSqlParser) {
		this.defaultSqlParser = defaultSqlParser;
	}

	public SystemConfig() {
		this.serverPort = DEFAULT_PORT;
		this.managerPort = DEFAULT_MANAGER_PORT;
		this.charset = DEFAULT_CHARSET;
		this.processors = DEFAULT_PROCESSORS;

		processorBufferChunk = DEFAULT_BUFFER_CHUNK_SIZE;
		this.processorExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS * 2
				: 4;
		this.managerExecutor = 2;
		processorBufferPool = DEFAULT_BUFFER_CHUNK_SIZE * processors * 1000;
		this.processorBufferLocalPercent = 100;
		this.timerExecutor = 2;
		this.idleTimeout = DEFAULT_IDLE_TIMEOUT;
		this.processorCheckPeriod = DEFAULT_PROCESSOR_CHECK_PERIOD;
		this.dataNodeIdleCheckPeriod = DEFAULT_DATANODE_IDLE_CHECK_PERIOD;
		this.dataNodeHeartbeatPeriod = DEFAULT_DATANODE_HEARTBEAT_PERIOD;
		this.clusterHeartbeatUser = DEFAULT_CLUSTER_HEARTBEAT_USER;
		this.clusterHeartbeatPass = DEFAULT_CLUSTER_HEARTBEAT_PASS;
		this.clusterHeartbeatPeriod = DEFAULT_CLUSTER_HEARTBEAT_PERIOD;
		this.clusterHeartbeatTimeout = DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT;
		this.clusterHeartbeatRetry = DEFAULT_CLUSTER_HEARTBEAT_RETRY;
		this.txIsolation = Isolations.REPEATED_READ;
		this.parserCommentVersion = DEFAULT_PARSER_COMMENT_VERSION;
		this.sqlRecordCount = DEFAULT_SQL_RECORD_COUNT;
		this.glableTableCheckPeriod = DEFAULT_GLOBAL_TABLE_CHECK_PERIOD;

	}
	
	public long getGlableTableCheckPeriod() {
		return glableTableCheckPeriod;
	}

	public void setGlableTableCheckPeriod(long glableTableCheckPeriod) {
		this.glableTableCheckPeriod = glableTableCheckPeriod;
	}

	public String getSqlInterceptor() {
		return sqlInterceptor;
	}

	public void setSqlInterceptor(String sqlInterceptor) {
		this.sqlInterceptor = sqlInterceptor;
	}

	public int getSequnceHandlerType() {
		return sequnceHandlerType;
	}

	public void setSequnceHandlerType(int sequnceHandlerType) {
		this.sequnceHandlerType = sequnceHandlerType;
	}

	public int getPacketHeaderSize() {
		return packetHeaderSize;
	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public int getCatletClassCheckSeconds() {
		return catletClassCheckSeconds;
	}

	public void setCatletClassCheckSeconds(int catletClassCheckSeconds) {
		this.catletClassCheckSeconds = catletClassCheckSeconds;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public int getFrontWriteQueueSize() {
		return frontWriteQueueSize;
	}

	public void setFrontWriteQueueSize(int frontWriteQueueSize) {
		this.frontWriteQueueSize = frontWriteQueueSize;
	}

	public String getBindIp() {
		return bindIp;
	}

	public void setBindIp(String bindIp) {
		this.bindIp = bindIp;
	}

	public int getDefaultMaxLimit() {
		return defaultMaxLimit;
	}

	public void setDefaultMaxLimit(int defaultMaxLimit) {
		this.defaultMaxLimit = defaultMaxLimit;
	}

	public static String getHomePath() {
		String home = System.getProperty(SystemConfig.SYS_HOME);
		if (home != null) {
			if (home.endsWith(File.pathSeparator)) {
				home = home.substring(0, home.length() - 1);
				System.setProperty(SystemConfig.SYS_HOME, home);
			}
		}

		// MYCAT_HOME为空，默认尝试设置为当前目录或上级目录。BEN
		if (home == null) {
			try {
				String path = new File("..").getCanonicalPath().replaceAll(
						"\\\\", "/");
				File conf = new File(path + "/conf");
				if (conf.exists() && conf.isDirectory()) {
					home = path;
				} else {
					path = new File(".").getCanonicalPath().replaceAll("\\\\",
							"/");
					conf = new File(path + "/conf");
					if (conf.exists() && conf.isDirectory()) {
						home = path;
					}
				}

				if (home != null) {
					System.setProperty(SystemConfig.SYS_HOME, home);
				}
			} catch (IOException e) {
				// 如出错，则忽略。
			}
		}

		return home;
	}

	public int getUseCompression() {
		return useCompression;
	}

	public void setUseCompression(int useCompression) {
		this.useCompression = useCompression;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public int getManagerPort() {
		return managerPort;
	}

	public void setManagerPort(int managerPort) {
		this.managerPort = managerPort;
	}

	public int getProcessors() {
		return processors;
	}

	public void setProcessors(int processors) {
		this.processors = processors;
	}

	public int getProcessorExecutor() {
		return processorExecutor;
	}

	public void setProcessorExecutor(int processorExecutor) {
		this.processorExecutor = processorExecutor;
	}

	public int getManagerExecutor() {
		return managerExecutor;
	}

	public void setManagerExecutor(int managerExecutor) {
		this.managerExecutor = managerExecutor;
	}

	public int getTimerExecutor() {
		return timerExecutor;
	}

	public void setTimerExecutor(int timerExecutor) {
		this.timerExecutor = timerExecutor;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public long getProcessorCheckPeriod() {
		return processorCheckPeriod;
	}

	public void setProcessorCheckPeriod(long processorCheckPeriod) {
		this.processorCheckPeriod = processorCheckPeriod;
	}

	public long getDataNodeIdleCheckPeriod() {
		return dataNodeIdleCheckPeriod;
	}

	public void setDataNodeIdleCheckPeriod(long dataNodeIdleCheckPeriod) {
		this.dataNodeIdleCheckPeriod = dataNodeIdleCheckPeriod;
	}

	public long getDataNodeHeartbeatPeriod() {
		return dataNodeHeartbeatPeriod;
	}

	public void setDataNodeHeartbeatPeriod(long dataNodeHeartbeatPeriod) {
		this.dataNodeHeartbeatPeriod = dataNodeHeartbeatPeriod;
	}

	public String getClusterHeartbeatUser() {
		return clusterHeartbeatUser;
	}

	public void setClusterHeartbeatUser(String clusterHeartbeatUser) {
		this.clusterHeartbeatUser = clusterHeartbeatUser;
	}

	public long getSqlExecuteTimeout() {
		return sqlExecuteTimeout;
	}

	public void setSqlExecuteTimeout(long sqlExecuteTimeout) {
		this.sqlExecuteTimeout = sqlExecuteTimeout;
	}

	public String getClusterHeartbeatPass() {
		return clusterHeartbeatPass;
	}

	public void setClusterHeartbeatPass(String clusterHeartbeatPass) {
		this.clusterHeartbeatPass = clusterHeartbeatPass;
	}

	public long getClusterHeartbeatPeriod() {
		return clusterHeartbeatPeriod;
	}

	public void setClusterHeartbeatPeriod(long clusterHeartbeatPeriod) {
		this.clusterHeartbeatPeriod = clusterHeartbeatPeriod;
	}

	public long getClusterHeartbeatTimeout() {
		return clusterHeartbeatTimeout;
	}

	public void setClusterHeartbeatTimeout(long clusterHeartbeatTimeout) {
		this.clusterHeartbeatTimeout = clusterHeartbeatTimeout;
	}

	public int getFrontsocketsorcvbuf() {
		return frontSocketSoRcvbuf;
	}

	public int getFrontsocketsosndbuf() {
		return frontSocketSoSndbuf;
	}

	public int getBacksocketsorcvbuf() {
		return backSocketSoRcvbuf;
	}

	public int getBacksocketsosndbuf() {
		return backSocketSoSndbuf;
	}

	public int getClusterHeartbeatRetry() {
		return clusterHeartbeatRetry;
	}

	public void setClusterHeartbeatRetry(int clusterHeartbeatRetry) {
		this.clusterHeartbeatRetry = clusterHeartbeatRetry;
	}

	public int getTxIsolation() {
		return txIsolation;
	}

	public void setTxIsolation(int txIsolation) {
		this.txIsolation = txIsolation;
	}

	public int getParserCommentVersion() {
		return parserCommentVersion;
	}

	public void setParserCommentVersion(int parserCommentVersion) {
		this.parserCommentVersion = parserCommentVersion;
	}

	public int getSqlRecordCount() {
		return sqlRecordCount;
	}

	public void setSqlRecordCount(int sqlRecordCount) {
		this.sqlRecordCount = sqlRecordCount;
	}

	public long getProcessorBufferPool() {
		return processorBufferPool;
	}

	public void setProcessorBufferPool(long processorBufferPool) {
		this.processorBufferPool = processorBufferPool;
	}

	public int getProcessorBufferChunk() {
		return processorBufferChunk;
	}

	public void setProcessorBufferChunk(int processorBufferChunk) {
		this.processorBufferChunk = processorBufferChunk;
	}

	public int getFrontSocketSoRcvbuf() {
		return frontSocketSoRcvbuf;
	}

	public void setFrontSocketSoRcvbuf(int frontSocketSoRcvbuf) {
		this.frontSocketSoRcvbuf = frontSocketSoRcvbuf;
	}

	public int getFrontSocketSoSndbuf() {
		return frontSocketSoSndbuf;
	}

	public void setFrontSocketSoSndbuf(int frontSocketSoSndbuf) {
		this.frontSocketSoSndbuf = frontSocketSoSndbuf;
	}

	public int getBackSocketSoRcvbuf() {
		return backSocketSoRcvbuf;
	}

	public void setBackSocketSoRcvbuf(int backSocketSoRcvbuf) {
		this.backSocketSoRcvbuf = backSocketSoRcvbuf;
	}

	public int getBackSocketSoSndbuf() {
		return backSocketSoSndbuf;
	}

	public void setBackSocketSoSndbuf(int backSocketSoSndbuf) {
		this.backSocketSoSndbuf = backSocketSoSndbuf;
	}

	public int getFrontSocketNoDelay() {
		return frontSocketNoDelay;
	}

	public void setFrontSocketNoDelay(int frontSocketNoDelay) {
		this.frontSocketNoDelay = frontSocketNoDelay;
	}

	public int getBackSocketNoDelay() {
		return backSocketNoDelay;
	}

	public void setBackSocketNoDelay(int backSocketNoDelay) {
		this.backSocketNoDelay = backSocketNoDelay;
	}

	public int getMaxStringLiteralLength() {
		return maxStringLiteralLength;
	}

	public void setMaxStringLiteralLength(int maxStringLiteralLength) {
		this.maxStringLiteralLength = maxStringLiteralLength;
	}

	public int getMutiNodeLimitType() {
		return mutiNodeLimitType;
	}

	public void setMutiNodeLimitType(int mutiNodeLimitType) {
		this.mutiNodeLimitType = mutiNodeLimitType;
	}

	public int getMutiNodePatchSize() {
		return mutiNodePatchSize;
	}

	public void setMutiNodePatchSize(int mutiNodePatchSize) {
		this.mutiNodePatchSize = mutiNodePatchSize;
	}

	public int getProcessorBufferLocalPercent() {
		return processorBufferLocalPercent;
	}

	public void setProcessorBufferLocalPercent(int processorBufferLocalPercent) {
		this.processorBufferLocalPercent = processorBufferLocalPercent;
	}

	public String getSqlInterceptorType() {
		return sqlInterceptorType;
	}

	public void setSqlInterceptorType(String sqlInterceptorType) {
		this.sqlInterceptorType = sqlInterceptorType;
	}

	public String getSqlInterceptorFile() {
		return sqlInterceptorFile;
	}

	public void setSqlInterceptorFile(String sqlInterceptorFile) {
		this.sqlInterceptorFile = sqlInterceptorFile;
	}

	public int getUsingAIO() {
		return usingAIO;
	}

	public void setUsingAIO(int usingAIO) {
		this.usingAIO = usingAIO;
	}

	public int getMycatNodeId() {
		return mycatNodeId;
	}

	public void setMycatNodeId(int mycatNodeId) {
		this.mycatNodeId = mycatNodeId;
	}

	public boolean isGlobalTableCheckSwitchOn() {
		return globalTableCheckSwitchOn;
	}

	public void setGlobalTableCheckSwitchOn(boolean globalTableCheckSwitchOn) {
		this.globalTableCheckSwitchOn = globalTableCheckSwitchOn;
	}

	@Override
	public String toString() {
		return "SystemConfig [processorBufferLocalPercent="
				+ processorBufferLocalPercent + ", frontSocketSoRcvbuf="
				+ frontSocketSoRcvbuf + ", frontSocketSoSndbuf="
				+ frontSocketSoSndbuf + ", backSocketSoRcvbuf="
				+ backSocketSoRcvbuf + ", backSocketSoSndbuf="
				+ backSocketSoSndbuf + ", frontSocketNoDelay="
				+ frontSocketNoDelay + ", backSocketNoDelay="
				+ backSocketNoDelay + ", maxStringLiteralLength="
				+ maxStringLiteralLength + ", frontWriteQueueSize="
				+ frontWriteQueueSize + ", bindIp=" + bindIp + ", serverPort="
				+ serverPort + ", managerPort=" + managerPort + ", charset="
				+ charset + ", processors=" + processors
				+ ", processorExecutor=" + processorExecutor
				+ ", timerExecutor=" + timerExecutor + ", managerExecutor="
				+ managerExecutor + ", idleTimeout=" + idleTimeout
				+ ", catletClassCheckSeconds=" + catletClassCheckSeconds
				+ ", sqlExecuteTimeout=" + sqlExecuteTimeout
				+ ", processorCheckPeriod=" + processorCheckPeriod
				+ ", dataNodeIdleCheckPeriod=" + dataNodeIdleCheckPeriod
				+ ", dataNodeHeartbeatPeriod=" + dataNodeHeartbeatPeriod
				+ ", clusterHeartbeatUser=" + clusterHeartbeatUser
				+ ", clusterHeartbeatPass=" + clusterHeartbeatPass
				+ ", clusterHeartbeatPeriod=" + clusterHeartbeatPeriod
				+ ", globalTableCheckSwitchOn=" + globalTableCheckSwitchOn
				+ ", glableTableCheckPeriod=" + glableTableCheckPeriod
				+ ", clusterHeartbeatTimeout=" + clusterHeartbeatTimeout
				+ ", clusterHeartbeatRetry=" + clusterHeartbeatRetry
				+ ", txIsolation=" + txIsolation + ", parserCommentVersion="
				+ parserCommentVersion + ", sqlRecordCount=" + sqlRecordCount
				+ ", processorBufferPool=" + processorBufferPool
				+ ", processorBufferChunk=" + processorBufferChunk
				+ ", defaultMaxLimit=" + defaultMaxLimit
				+ ", sequnceHandlerType=" + sequnceHandlerType
				+ ", sqlInterceptor=" + sqlInterceptor
				+ ", sqlInterceptorType=" + sqlInterceptorType
				+ ", sqlInterceptorFile=" + sqlInterceptorFile
				+ ", mutiNodeLimitType=" + mutiNodeLimitType
				+ ", mutiNodePatchSize=" + mutiNodePatchSize
				+ ", defaultSqlParser=" + defaultSqlParser + ", usingAIO="
				+ usingAIO + ", packetHeaderSize=" + packetHeaderSize
				+ ", maxPacketSize=" + maxPacketSize + ", mycatNodeId="
				+ mycatNodeId + "]";
	}

}