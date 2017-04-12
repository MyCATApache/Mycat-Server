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
package io.mycat.config.model;

import java.io.File;
import java.io.IOException;

import io.mycat.config.Isolations;

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

	private static final String DEFAULT_SQL_PARSER = "druidparser";// fdbparser, druidparser
	private static final short DEFAULT_BUFFER_CHUNK_SIZE = 4096;
	private static final int DEFAULT_BUFFER_POOL_PAGE_SIZE = 512*1024*4;
	private static final short DEFAULT_BUFFER_POOL_PAGE_NUMBER = 64;
	private int processorBufferLocalPercent;
	private static final int DEFAULT_PROCESSORS = Runtime.getRuntime().availableProcessors();
	private int frontSocketSoRcvbuf = 1024 * 1024;
	private int frontSocketSoSndbuf = 4 * 1024 * 1024;
	private int backSocketSoRcvbuf = 4 * 1024 * 1024;// mysql 5.6
														// net_buffer_length
														// defaut 4M
    
	private final  static String RESERVED_SYSTEM_MEMORY_BYTES = "384m";
	private final static String MEMORY_PAGE_SIZE = "1m";
	private final static String SPILLS_FILE_BUFFER_SIZE = "2K";
	private final static String DATANODE_SORTED_TEMP_DIR = "datanode";
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
	private static final boolean DEFAULT_USE_ZK_SWITCH = true;
	private int maxStringLiteralLength = 65535;
	private int frontWriteQueueSize = 2048;
	private String bindIp = "0.0.0.0";
	private String fakeMySQLVersion = null;
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
	private long clusterHeartbeatTimeout;
	private int clusterHeartbeatRetry;
	private int txIsolation;
	private int parserCommentVersion;
	private int sqlRecordCount;

	// a page size
	private int bufferPoolPageSize;

	//minimum allocation unit
	private short bufferPoolChunkSize;
	
	// buffer pool page number 
	private short bufferPoolPageNumber;
	
	//大结果集阈值，默认512kb
	private int maxResultSet=512*1024;
	//大结果集拒绝策略次数过滤限制,默认10次
	private int bigResultSizeSqlCount=10;
	//大结果集拒绝策咯，bufferpool使用率阈值(0-100)，默认80%
	private int  bufferUsagePercent=80;
	//大结果集保护策咯，0:不开启,1:级别1为在当前mucat bufferpool
	//使用率大于bufferUsagePercent阈值时，拒绝超过defaultBigResultSizeSqlCount
	//sql次数阈值并且符合超过大结果集阈值maxResultSet的所有sql
	//默认值0
	private int  flowControlRejectStrategy=0;
	//清理大结果集记录周期
	private long clearBigSqLResultSetMapMs=10*60*1000;

	private int defaultMaxLimit = DEFAULT_MAX_LIMIT;
	public static final int SEQUENCEHANDLER_LOCALFILE = 0;
	public static final int SEQUENCEHANDLER_MYSQLDB = 1;
	public static final int SEQUENCEHANDLER_LOCAL_TIME = 2;
	public static final int SEQUENCEHANDLER_ZK_DISTRIBUTED = 3;
	public static final int SEQUENCEHANDLER_ZK_GLOBAL_INCREMENT = 4;
	/*
	 * 注意！！！ 目前mycat支持的MySQL版本，如果后续有新的MySQL版本,请添加到此数组， 对于MySQL的其他分支，
	 * 比如MariaDB目前版本号已经到10.1.x，但是其驱动程序仍然兼容官方的MySQL,因此这里版本号只需要MySQL官方的版本号即可。
	 */
	public static final String[] MySQLVersions = { "5.5", "5.6", "5.7" };
	private int sequnceHandlerType = SEQUENCEHANDLER_LOCALFILE;
	private String sqlInterceptor = "io.mycat.server.interceptor.impl.DefaultSqlInterceptor";
	private String sqlInterceptorType = "select";
	private String sqlInterceptorFile = System.getProperty("user.dir")+"/logs/sql.txt";
	public static final int MUTINODELIMIT_SMALL_DATA = 0;
	public static final int MUTINODELIMIT_LAR_DATA = 1;
	private int mutiNodeLimitType = MUTINODELIMIT_SMALL_DATA;

	public static final int MUTINODELIMIT_PATCH_SIZE = 100;
	private int mutiNodePatchSize = MUTINODELIMIT_PATCH_SIZE;

	private String defaultSqlParser = DEFAULT_SQL_PARSER;
	private int usingAIO = 0;
	private int packetHeaderSize = 4;
	private int maxPacketSize = 16 * 1024 * 1024;
	private int mycatNodeId=1;
	private int useCompression =0;	
	private int useSqlStat = 1;
	
	// 是否使用HandshakeV10Packet来与client进行通讯, 1:是 , 0:否(使用HandshakePacket)
	// 使用HandshakeV10Packet为的是兼容高版本的jdbc驱动, 后期稳定下来考虑全部采用HandshakeV10Packet来通讯
	private int useHandshakeV10 = 0;

	//处理分布式事务开关，默认为不过滤分布式事务
	private int handleDistributedTransactions = 0;

	private int checkTableConsistency = 0;
	private long checkTableConsistencyPeriod = CHECKTABLECONSISTENCYPERIOD;
	private final static long CHECKTABLECONSISTENCYPERIOD = 1 * 60 * 1000;

	private int processorBufferPoolType = 0;

	// 全局表一致性检测任务，默认24小时调度一次
	private static final long DEFAULT_GLOBAL_TABLE_CHECK_PERIOD = 24 * 60 * 60 * 1000L;
	private int useGlobleTableCheck = 1;	// 全局表一致性检查开关
	
	private long glableTableCheckPeriod;

	/**
	 * Mycat 使用 Off Heap For Merge/Order/Group/Limit计算相关参数
	 */


	/**
	 * 是否启用Off Heap for Merge  1-启用，0-不启用
	 */
	private int useOffHeapForMerge;

	/**
	 *页大小,对应MemoryBlock的大小，单位为M
	 */
	private String memoryPageSize;


	/**
	 * DiskRowWriter写磁盘是临时写Buffer，单位为K
	 */
	private String spillsFileBufferSize;

	/**
	 * 启用结果集流输出，不经过merge模块,
	 */
	private int useStreamOutput;

	/**
	 * 该变量仅在Merge使用On Heap
	 * 内存方式时起作用，如果使用Off Heap内存方式
	 * 那么可以认为-Xmx就是系统预留内存。
	 * 在On Heap上给系统预留的内存，
	 * 主要供新小对象创建，JAVA简单数据结构使用
	 * 以保证在On Heap上大结果集计算时情况，能快速响应其他
	 * 连接操作。
	 */
	private String systemReserveMemorySize;

	private String XARecoveryLogBaseDir;

	private String XARecoveryLogBaseName;

	/**
	 * 排序时，内存不够时，将已经排序的结果集
	 * 写入到临时目录
	 */
	private String dataNodeSortedTempDir;

	/**
	 * 是否启用zk切换
	 */
	private boolean	useZKSwitch=DEFAULT_USE_ZK_SWITCH;

	
 	/**
 	 * huangyiming add
	 * 无密码登陆标示, 0:否,1:是,默认为0
	 */
	private int nonePasswordLogin = DEFAULT_NONEPASSWORDLOGIN ;

	private final static int DEFAULT_NONEPASSWORDLOGIN = 0;
	
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
		this.bufferPoolPageSize = DEFAULT_BUFFER_POOL_PAGE_SIZE;
		this.bufferPoolChunkSize = DEFAULT_BUFFER_CHUNK_SIZE;
		
		/**
		 * 大结果集时 需增大 network buffer pool pages.
		 */
		this.bufferPoolPageNumber = (short) (DEFAULT_PROCESSORS*20);

		this.processorExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS * 2 : 4;
		this.managerExecutor = 2;

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
		this.useOffHeapForMerge = 1;
		this.memoryPageSize = MEMORY_PAGE_SIZE;
		this.spillsFileBufferSize = SPILLS_FILE_BUFFER_SIZE;
		this.useStreamOutput = 0;
		this.systemReserveMemorySize = RESERVED_SYSTEM_MEMORY_BYTES;
		this.dataNodeSortedTempDir = System.getProperty("user.dir");
		this.XARecoveryLogBaseDir = SystemConfig.getHomePath()+"/tmlogs/";
		this.XARecoveryLogBaseName ="tmlog";
	}

	public String getDataNodeSortedTempDir() {
		return dataNodeSortedTempDir;
	}

	public int getUseOffHeapForMerge() {
		return useOffHeapForMerge;
	}

	public void setUseOffHeapForMerge(int useOffHeapForMerge) {
		this.useOffHeapForMerge = useOffHeapForMerge;
	}

	public String getMemoryPageSize() {
		return memoryPageSize;
	}

	public void setMemoryPageSize(String memoryPageSize) {
		this.memoryPageSize = memoryPageSize;
	}

	public String getSpillsFileBufferSize() {
		return spillsFileBufferSize;
	}

	public void setSpillsFileBufferSize(String spillsFileBufferSize) {
		this.spillsFileBufferSize = spillsFileBufferSize;
	}

	public int getUseStreamOutput() {
		return useStreamOutput;
	}

	public void setUseStreamOutput(int useStreamOutput) {
		this.useStreamOutput = useStreamOutput;
	}

	public String getSystemReserveMemorySize() {
		return systemReserveMemorySize;
	}

	public void setSystemReserveMemorySize(String systemReserveMemorySize) {
		this.systemReserveMemorySize = systemReserveMemorySize;
	}

	public boolean isUseZKSwitch() {
		return useZKSwitch;
	}

	public void setUseZKSwitch(boolean useZKSwitch) {
		this.useZKSwitch = useZKSwitch;
	}

	public String getXARecoveryLogBaseDir() {
		return XARecoveryLogBaseDir;
	}

	public void setXARecoveryLogBaseDir(String XARecoveryLogBaseDir) {
		this.XARecoveryLogBaseDir = XARecoveryLogBaseDir;
	}

	public String getXARecoveryLogBaseName() {
		return XARecoveryLogBaseName;
	}

	public void setXARecoveryLogBaseName(String XARecoveryLogBaseName) {
		this.XARecoveryLogBaseName = XARecoveryLogBaseName;
	}

	public int getUseGlobleTableCheck() {
		return useGlobleTableCheck;
	}

	public void setUseGlobleTableCheck(int useGlobleTableCheck) {
		this.useGlobleTableCheck = useGlobleTableCheck;
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
		if (home != null
				&& home.endsWith(File.pathSeparator)) {
				home = home.substring(0, home.length() - 1);
				System.setProperty(SystemConfig.SYS_HOME, home);
		}

		// MYCAT_HOME为空，默认尝试设置为当前目录或上级目录。BEN
		if(home == null) {
			try {
				String path = new File("..").getCanonicalPath().replaceAll("\\\\", "/");
				File conf = new File(path+"/conf");
				if(conf.exists() && conf.isDirectory()) {
					home = path;
				} else {
					path = new File(".").getCanonicalPath().replaceAll("\\\\", "/");
					conf = new File(path+"/conf");
					if(conf.exists() && conf.isDirectory()) {
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
	
	// 是否使用SQL统计
	public int getUseSqlStat() 
	{
		return useSqlStat;
	}
	
	public void setUseSqlStat(int useSqlStat) 
	{
		this.useSqlStat = useSqlStat;
	}

	public int getUseCompression()
	{
		return useCompression;
	}

	public void setUseCompression(int useCompression)
	{
		this.useCompression = useCompression;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getFakeMySQLVersion() {
		return fakeMySQLVersion;
	}

	public void setFakeMySQLVersion(String mysqlVersion) {
		this.fakeMySQLVersion = mysqlVersion;
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


	public short getBufferPoolChunkSize() {
		return bufferPoolChunkSize;
	}

	public void setBufferPoolChunkSize(short bufferPoolChunkSize) {
		this.bufferPoolChunkSize = bufferPoolChunkSize;
	}
	
	public int getMaxResultSet() {
		return maxResultSet;
	}

	public void setMaxResultSet(int maxResultSet) {
		this.maxResultSet = maxResultSet;
	}

	public int getBigResultSizeSqlCount() {
		return bigResultSizeSqlCount;
	}

	public void setBigResultSizeSqlCount(int bigResultSizeSqlCount) {
		this.bigResultSizeSqlCount = bigResultSizeSqlCount;
	}

	public int getBufferUsagePercent() {
		return bufferUsagePercent;
	}

	public void setBufferUsagePercent(int bufferUsagePercent) {
		this.bufferUsagePercent = bufferUsagePercent;
	}

	public int getFlowControlRejectStrategy() {
		return flowControlRejectStrategy;
	}

	public void setFlowControlRejectStrategy(int flowControlRejectStrategy) {
		this.flowControlRejectStrategy = flowControlRejectStrategy;
	}

	public long getClearBigSqLResultSetMapMs() {
		return clearBigSqLResultSetMapMs;
	}

	public void setClearBigSqLResultSetMapMs(long clearBigSqLResultSetMapMs) {
		this.clearBigSqLResultSetMapMs = clearBigSqLResultSetMapMs;
	}

	public int getBufferPoolPageSize() {
		return bufferPoolPageSize;
	}

	public void setBufferPoolPageSize(int bufferPoolPageSize) {
		this.bufferPoolPageSize = bufferPoolPageSize;
	}

	public short getBufferPoolPageNumber() {
		return bufferPoolPageNumber;
	}

	public void setBufferPoolPageNumber(short bufferPoolPageNumber) {
		this.bufferPoolPageNumber = bufferPoolPageNumber;
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
				+ ", clusterHeartbeatTimeout=" + clusterHeartbeatTimeout
				+ ", clusterHeartbeatRetry=" + clusterHeartbeatRetry
				+ ", txIsolation=" + txIsolation + ", parserCommentVersion="
				+ parserCommentVersion + ", sqlRecordCount=" + sqlRecordCount
				+ ", bufferPoolPageSize=" + bufferPoolPageSize
				+ ", bufferPoolChunkSize=" + bufferPoolChunkSize
				+ ", bufferPoolPageNumber=" + bufferPoolPageNumber
				+ ", maxResultSet=" +maxResultSet
				+ ", bigResultSizeSqlCount="+bigResultSizeSqlCount
				+ ", bufferUsagePercent="+bufferUsagePercent
				+ ", flowControlRejectStrategy="+flowControlRejectStrategy
				+ ", clearBigSqLResultSetMapMs="+clearBigSqLResultSetMapMs
				+ ", defaultMaxLimit=" + defaultMaxLimit
				+ ", sequnceHandlerType=" + sequnceHandlerType
				+ ", sqlInterceptor=" + sqlInterceptor
				+ ", sqlInterceptorType=" + sqlInterceptorType
				+ ", sqlInterceptorFile=" + sqlInterceptorFile
				+ ", mutiNodeLimitType=" + mutiNodeLimitType 
				+ ", mutiNodePatchSize=" + mutiNodePatchSize 
				+ ", defaultSqlParser=" + defaultSqlParser
				+ ", usingAIO=" + usingAIO 
				+ ", packetHeaderSize=" + packetHeaderSize 
				+ ", maxPacketSize=" + maxPacketSize
				+ ", mycatNodeId=" + mycatNodeId + "]";
	}


	public int getCheckTableConsistency() {
		return checkTableConsistency;
	}

	public void setCheckTableConsistency(int checkTableConsistency) {
		this.checkTableConsistency = checkTableConsistency;
	}

	public long getCheckTableConsistencyPeriod() {
		return checkTableConsistencyPeriod;
	}

	public void setCheckTableConsistencyPeriod(long checkTableConsistencyPeriod) {
		this.checkTableConsistencyPeriod = checkTableConsistencyPeriod;
	}

	public int getProcessorBufferPoolType() {
		return processorBufferPoolType;
	}

	public void setProcessorBufferPoolType(int processorBufferPoolType) {
		this.processorBufferPoolType = processorBufferPoolType;
	}

	public int getHandleDistributedTransactions() {
		return handleDistributedTransactions;
	}

	public void setHandleDistributedTransactions(int handleDistributedTransactions) {
		this.handleDistributedTransactions = handleDistributedTransactions;
	}

	public int getUseHandshakeV10() {
		return useHandshakeV10;
	}

	public void setUseHandshakeV10(int useHandshakeV10) {
		this.useHandshakeV10 = useHandshakeV10;
	}

	public int getNonePasswordLogin() {
		return nonePasswordLogin;
	}

	public void setNonePasswordLogin(int nonePasswordLogin) {
		this.nonePasswordLogin = nonePasswordLogin;
	}
	
	
}
