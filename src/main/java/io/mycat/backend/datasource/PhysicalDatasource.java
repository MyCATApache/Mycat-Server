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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.ConMap;
import io.mycat.backend.ConQueue;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import io.mycat.backend.mysql.nio.handler.DelegateResponseHandler;
import io.mycat.backend.mysql.nio.handler.NewConnectionRespHandler;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.Alarms;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.DataHostConfig;
import io.mycat.util.TimeUtil;


public abstract class PhysicalDatasource {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDatasource.class);

	private final String name;
	private final int size;
	private final DBHostConfig config;
	private final ConMap conMap = new ConMap();
	private DBHeartbeat heartbeat;
	private final boolean readNode;
	private volatile long heartbeatRecoveryTime;
	private final DataHostConfig hostConfig;
	private final ConnectionHeartBeatHandler conHeartBeatHanler = new ConnectionHeartBeatHandler();
	private PhysicalDBPool dbPool;
	private volatile long totalConnectionCount = 0L;
	//判断是否需要同步 increamentCount
	private LongAdder increamentCount = new LongAdder();
	private long preIncrementCount = 0;

	
	// 添加DataSource读计数
	private AtomicLong readCount = new AtomicLong(0);
	
	// 添加DataSource写计数
	private AtomicLong writeCount = new AtomicLong(0);
	
	
	/** 
	 *   edit by dingw at 2017.06.08
	 *   @see https://github.com/MyCATApache/Mycat-Server/issues/1524
	 *   
	 */
	// 当前活动连接
	//private volatile AtomicInteger activeCount = new AtomicInteger(0);
	
	// 当前存活的总连接数,为什么不直接使用activeCount,主要是因为连接的创建是异步完成的
	//private volatile AtomicInteger totalConnection = new AtomicInteger(0);
	
	/**
	 * 由于在Mycat中，returnCon被多次调用（与takeCon并没有成对调用）导致activeCount、totalConnection容易出现负数
	 */
	//private static final String TAKE_CONNECTION_FLAG = "1";
	//private ConcurrentMap<Long /* ConnectionId */, String /* 常量1*/> takeConnectionContext = new ConcurrentHashMap<>();

	

	public PhysicalDatasource(DBHostConfig config, DataHostConfig hostConfig,
			boolean isReadNode) {
		this.size = config.getMaxCon();
		this.config = config;
		this.name = config.getHostName();
		this.hostConfig = hostConfig;
		heartbeat = this.createHeartBeat();
		this.readNode = isReadNode;
	}

	public boolean isMyConnection(BackendConnection con) {
		if (con instanceof MySQLConnection) {
			return ((MySQLConnection) con).getPool() == this;
		} else {
			return false;
		}

	}

	public long getReadCount() {
		return readCount.get();
	}

	public void setReadCount() {
		readCount.addAndGet(1);
	}

	public long getWriteCount() {
		return writeCount.get();
	}

	public void setWriteCount() {
		writeCount.addAndGet(1);
	}

	public DataHostConfig getHostConfig() {
		return hostConfig;
	}

	public boolean isReadNode() {
		return readNode;
	}

	public int getSize() {
		return size;
	}

	public void setDbPool(PhysicalDBPool dbPool) {
		this.dbPool = dbPool;
	}

	public PhysicalDBPool getDbPool() {
		return dbPool;
	}

	public abstract DBHeartbeat createHeartBeat();

	public String getName() {
		return name;
	}

	public long getExecuteCount() {
		long executeCount = 0;
		for (ConQueue queue : conMap.getAllConQueue()) {
			executeCount += queue.getExecuteCount();

		}
		return executeCount;
	}

	public long getExecuteCountForSchema(String schema) {
		return conMap.getSchemaConQueue(schema).getExecuteCount();

	}

	public int getActiveCountForSchema(String schema) {
		return conMap.getActiveCountForSchema(schema, this);
	}

	public int getIdleCountForSchema(String schema) {
		ConQueue queue = conMap.getSchemaConQueue(schema);
		int total = 0;
		total += queue.getAutoCommitCons().size()
				+ queue.getManCommitCons().size();
		return total;
	}

	public DBHeartbeat getHeartbeat() {
		return heartbeat;
	}

	public int getIdleCount() {
		int total = 0;
		for (ConQueue queue : conMap.getAllConQueue()) {
			total += queue.getAutoCommitCons().size()
					+ queue.getManCommitCons().size();
		}
		return total;
	}
	
	/**
	 * 该方法也不是非常精确，因为该操作也不是一个原子操作,相对getIdleCount高效与准确一些
	 * @return
	 */
//	public int getIdleCountSafe() {
//		return getTotalConnectionsSafe() - getActiveCountSafe();
//	}
	
	/**
	 * 是否需要继续关闭空闲连接
	 * @return
	 */
//	private boolean needCloseIdleConnection() {
//		return getIdleCountSafe() > hostConfig.getMinCon();
//	}

	private boolean validSchema(String schema) {
		String theSchema = schema;
		return theSchema != null && !"".equals(theSchema)
				&& !"snyn...".equals(theSchema);
	}

	private void checkIfNeedHeartBeat(
			LinkedList<BackendConnection> heartBeatCons, ConQueue queue,
			ConcurrentLinkedQueue<BackendConnection> checkLis,
			long hearBeatTime, long hearBeatTime2) {
		int maxConsInOneCheck = 10;
		Iterator<BackendConnection> checkListItor = checkLis.iterator();
		while (checkListItor.hasNext()) {
			BackendConnection con = checkListItor.next();
			if (con.isClosedOrQuit()) {
				checkListItor.remove();
				continue;
			}
			if (validSchema(con.getSchema())) {
				if (con.getLastTime() < hearBeatTime
						&& heartBeatCons.size() < maxConsInOneCheck) {
					if(checkLis.remove(con)) { 
						//如果移除成功，则放入到心跳连接中，如果移除失败，说明该连接已经被其他线程使用，忽略本次心跳检测
						con.setBorrowed(true);
						heartBeatCons.add(con);
					}
				}
			} else if (con.getLastTime() < hearBeatTime2) {
				// not valid schema conntion should close for idle
				// exceed 2*conHeartBeatPeriod
				// 同样，这里也需要先移除，避免被业务连接
				if(checkLis.remove(con)) { 
					con.close(" heart beate idle ");
				}
			}

		}

	}

	public int getIndex() {
		int currentIndex = 0;
		for (int i = 0; i < dbPool.getSources().length; i++) {
			PhysicalDatasource writeHostDatasource = dbPool.getSources()[i];
			if (writeHostDatasource.getName().equals(getName())) {
				currentIndex = i;
				break;
			}
		}
		return currentIndex;
	}

	public boolean isSalveOrRead() {
		int currentIndex = getIndex();
		if (currentIndex != dbPool.activedIndex || this.readNode) {
			return true;
		}
		return false;
	}

	public void heatBeatCheck(long timeout, long conHeartBeatPeriod) {
//		int ildeCloseCount = hostConfig.getMinCon() * 3;
		int maxConsInOneCheck = 5;
		LinkedList<BackendConnection> heartBeatCons = new LinkedList<BackendConnection>();

		long hearBeatTime = TimeUtil.currentTimeMillis() - conHeartBeatPeriod;
		long hearBeatTime2 = TimeUtil.currentTimeMillis() - 2
				* conHeartBeatPeriod;
		for (ConQueue queue : conMap.getAllConQueue()) {
			checkIfNeedHeartBeat(heartBeatCons, queue,
					queue.getAutoCommitCons(), hearBeatTime, hearBeatTime2);
			if (heartBeatCons.size() < maxConsInOneCheck) {
				checkIfNeedHeartBeat(heartBeatCons, queue,
						queue.getManCommitCons(), hearBeatTime, hearBeatTime2);
			} else if (heartBeatCons.size() >= maxConsInOneCheck) {
				break;
			}
		}

		if (!heartBeatCons.isEmpty()) {
			for (BackendConnection con : heartBeatCons) {
				conHeartBeatHanler
						.doHeartBeat(con, hostConfig.getHearbeatSQL());
			}
		}

		// check if there has timeouted heatbeat cons
		conHeartBeatHanler.abandTimeOuttedConns();
		int idleCons = getIdleCount();
		int activeCons = this.getActiveCount();
		int createCount = (hostConfig.getMinCon() - idleCons) / 3;
		// create if idle too little
		if ((createCount > 0) && (idleCons + activeCons < size)
				&& (idleCons < hostConfig.getMinCon())) {
			createByIdleLitte(idleCons, createCount);
		} else if (idleCons > hostConfig.getMinCon()) {
			closeByIdleMany(idleCons - hostConfig.getMinCon());
		} else {
			int activeCount = this.getActiveCount();
			if (activeCount > size) {
				StringBuilder s = new StringBuilder();
				s.append(Alarms.DEFAULT).append("DATASOURCE EXCEED [name=")
						.append(name).append(",active=");
				s.append(activeCount).append(",size=").append(size).append(']');
				LOGGER.warn(s.toString());
			}
		}
	}

	/**
	 * 
	 * @param ildeCloseCount
	 * 首先，从已创建的连接中选择本次心跳需要关闭的空闲连接数（由当前连接连接数-减去配置的最小连接数。
	 * 然后依次关闭这些连接。由于连接空闲心跳检测与业务是同时并发的，在心跳关闭阶段，可能有连接被使用，导致需要关闭的空闲连接数减少.
	 * 
	 * 所以每次关闭新连接时，先判断当前空闲连接数是否大于配置的最少空闲连接，如果为否，则结束本次关闭空闲连接操作。
	 * 该方法修改之前：
	 *      首先从ConnMap中获取 ildeCloseCount 个连接，然后关闭；在关闭中，可能又有连接被使用，导致可能多关闭一些链接，
	 *      导致相对频繁的创建新连接和关闭连接
	 *      
	 * 该方法修改之后：
	 *     ildeCloseCount 为预期要关闭的连接
	 *     使用循环操作，首先在关闭之前，先再一次判断是否需要关闭连接，然后每次从ConnMap中获取一个空闲连接，然后进行关闭
	 * edit by dingw at 2017.06.16
	 */
	private void closeByIdleMany(int ildeCloseCount) {
		LOGGER.info("too many ilde cons ,close some for datasouce  " + name);
		List<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(
				ildeCloseCount);
		for (ConQueue queue : conMap.getAllConQueue()) {
			readyCloseCons.addAll(queue.getIdleConsToClose(ildeCloseCount));
			if (readyCloseCons.size() >= ildeCloseCount) {
				break;
			}
		}

		for (BackendConnection idleCon : readyCloseCons) {
			if (idleCon.isBorrowed()) {
				LOGGER.warn("find idle con is using " + idleCon);
			}
			idleCon.close("too many idle con");
		}
		
//		LOGGER.info("too many ilde cons ,close some for datasouce  " + name);
//		
//		Iterator<ConQueue> conQueueIt = conMap.getAllConQueue().iterator();
//		ConQueue queue = null;
//		if(conQueueIt.hasNext()) {
//			queue = conQueueIt.next();
//		}
//		
//		for(int i = 0; i < ildeCloseCount; i ++ ) {
//			
//			if(!needCloseIdleConnection() || queue == null) {
//				break; //如果当时空闲连接数没有超过最小配置连接数，则结束本次连接关闭
//			}
//			
//			LOGGER.info("cur conns:" + getTotalConnectionsSafe() );
//			
//			BackendConnection idleCon = queue.takeIdleCon(false);
//			
//			while(idleCon == null && conQueueIt.hasNext()) {
//				queue = conQueueIt.next();
//				idleCon = queue.takeIdleCon(false);
//			}
//			
//			if(idleCon == null) { 
//				break;
//			}
//			
//			if (idleCon.isBorrowed() ) {
//				LOGGER.warn("find idle con is using " + idleCon);
//			}
//			idleCon.close("too many idle con");
//			
//		}
		
	}

	private void createByIdleLitte(int idleCons, int createCount) {
		LOGGER.info("create connections ,because idle connection not enough ,cur is "
				+ idleCons
				+ ", minCon is "
				+ hostConfig.getMinCon()
				+ " for "
				+ name);
		NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();

		final String[] schemas = dbPool.getSchemas();
		for (int i = 0; i < createCount; i++) {
			if (this.getActiveCount() + this.getIdleCount() >= size) {
				break;
			}
			try {
				// creat new connection
				this.createNewConnection(simpleHandler, null, schemas[i
						% schemas.length]);
			} catch (IOException e) {
				LOGGER.warn("create connection err " + e);
			}

		}
	}

	public int getActiveCount() {
		return this.conMap.getActiveCountForDs(this);
	}

	public long getTotalCount() {
		return totalConnectionCount + increamentCount.intValue();
	}

	public void calcTotalCount() {
		//当连接数增量开始变化的时候，先直接用increamentCount记录连接数，当一秒钟内不再有新增了之后，开始同步 totalConnectionCount
		if (preIncrementCount == increamentCount.longValue()) {
			long total = this.conMap.getTotalCountForDs(this);
			long inc = increamentCount.sumThenReset() - preIncrementCount;
			totalConnectionCount = total + inc;
			preIncrementCount = 0;

		} else {
			preIncrementCount = increamentCount.longValue();
		}
	}
	
	

	public void clearCons(String reason) {
		this.conMap.clearConnections(reason, this);
	}

	public void startHeartbeat() {
		heartbeat.start();
	}

	public void stopHeartbeat() {
		heartbeat.stop();
	}

	public void doHeartbeat() {		
		// 未到预定恢复时间，不执行心跳检测。
		if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
			return;
		}
		
		if (!heartbeat.isStop()) {
			try {
				heartbeat.heartbeat();
			} catch (Exception e) {
				LOGGER.error(name + " heartbeat error.", e);
			}
		}
	}

	private BackendConnection takeCon(BackendConnection conn,
			final ResponseHandler handler, final Object attachment,
			String schema) {

		conn.setBorrowed(true);

//		if(takeConnectionContext.putIfAbsent(conn.getId(), TAKE_CONNECTION_FLAG) == null) {
//			incrementActiveCountSafe();
//		}
		
		
		if (!conn.getSchema().equals(schema)) {
			// need do schema syn in before sql send
			conn.setSchema(schema);
		}
		ConQueue queue = conMap.getSchemaConQueue(schema);
		queue.incExecuteCount();
		conn.setAttachment(attachment);
		conn.setLastTime(System.currentTimeMillis()); // 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致sql执行失败
		handler.connectionAcquired(conn);
		return conn;
	}

	private void createNewConnection(final ResponseHandler handler,
			final Object attachment, final String schema) throws IOException {		
		// aysn create connection
		final AtomicBoolean hasError = new AtomicBoolean(false);

		MycatServer.getInstance().getBusinessExecutor().execute(new Runnable() {
			public void run() {
				try {
					createNewConnection(new DelegateResponseHandler(handler) {
						@Override
						public void connectionError(Throwable e, BackendConnection conn) {
							if(hasError.compareAndSet(false, true)) {
								handler.connectionError(e, conn);
							} else {
								LOGGER.info("connection connectionError ");
							}
						}

						@Override
						public void connectionAcquired(BackendConnection conn) {
							LOGGER.info("connection id is "+conn.getId());
							takeCon(conn, handler, attachment, schema);
						}
					}, schema);
				} catch (IOException e) {
					if(hasError.compareAndSet(false, true)) {
						handler.connectionError(e, null);
					} else {
						LOGGER.info("connection connectionError ");
					}
				}
			}
		});
	}

	public void getConnection(String schema, boolean autocommit,
			final ResponseHandler handler, final Object attachment)
			throws IOException {
		
		// 从当前连接map中拿取已建立好的后端连接
		BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
		if (con != null) {
			//如果不为空，则绑定对应前端请求的handler
			takeCon(con, handler, attachment, schema);
			return;	
			
		} else { // this.getActiveCount并不是线程安全的（严格上说该方法获取数量不准确），
//			int curTotalConnection = this.totalConnection.get();
//			while(curTotalConnection + 1 <= size) {
//				
//				if (this.totalConnection.compareAndSet(curTotalConnection, curTotalConnection + 1)) {
//					LOGGER.info("no ilde connection in pool,create new connection for "	+ this.name + " of schema " + schema);
//					createNewConnection(handler, attachment, schema);
//					return;
//				}
//				
//				curTotalConnection = this.totalConnection.get(); //CAS更新失败，则重新判断当前连接是否超过最大连接数
//				
//			}
//			
//			// 如果后端连接不足，立即失败,故直接抛出连接数超过最大连接异常
//			LOGGER.error("the max activeConnnections size can not be max than maxconnections:" + curTotalConnection);
//			throw new IOException("the max activeConnnections size can not be max than maxconnections:" + curTotalConnection);


			// 当前最大连接
			long activeCons = increamentCount.longValue()+totalConnectionCount;
			if (activeCons < size) {// 下一个连接大于最大连接数
				//提前increamentCount的操作
				increamentCount.increment();
				LOGGER.info("no ilde connection in pool "+System.identityHashCode(this)+" ,create new connection for "	+ this.name + " of schema " + schema + " totalConnectionCount: " + totalConnectionCount + " increamentCount: "+increamentCount);
				createNewConnection(handler, attachment, schema);
			} else { // create connection
				LOGGER.error("the max activeConnnections size can not be max than maxconnections");
				throw new IOException("the max activeConnnections size can not be max than maxconnections");
			}
		}
	}
	
	/**
	 * 是否超过最大连接数
	 * @return
	 */
//	private boolean exceedMaxConnections() {
//		return this.totalConnection.get() + 1 > size;
//	}
//	
//	public int decrementActiveCountSafe() {
//		return this.activeCount.decrementAndGet();
//	}
//	
//	public int incrementActiveCountSafe() {
//		return this.activeCount.incrementAndGet();
//	}
//	
//	public int getActiveCountSafe() {
//		return this.activeCount.get();
//	}
//	
//	public int getTotalConnectionsSafe() {
//		return this.totalConnection.get();
//	}
//	
//	public int decrementTotalConnectionsSafe() {
//		return this.totalConnection.decrementAndGet();
//	}
//	
//	public int incrementTotalConnectionSafe() {
//		return this.totalConnection.incrementAndGet();
//	}

	private void returnCon(BackendConnection c) {
		
		c.setAttachment(null);
		c.setBorrowed(false);
		c.setLastTime(TimeUtil.currentTimeMillis());
		ConQueue queue = this.conMap.getSchemaConQueue(c.getSchema());

		boolean ok = false;
		if (c.isAutocommit()) {
			ok = queue.getAutoCommitCons().offer(c);
		} else {
			ok = queue.getManCommitCons().offer(c);
		}
		
//		if(c.getId() > 0 && takeConnectionContext.remove(c.getId(), TAKE_CONNECTION_FLAG) ) {
//			decrementActiveCountSafe();
//		}
		
		if(!ok) {
			LOGGER.warn("can't return to pool ,so close con " + c);
			c.close("can't return to pool ");
			
		}
		
	}

	public void releaseChannel(BackendConnection c) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("release channel " + c);
		}
		// release connection
		returnCon(c);
	}

	public void connectionClosed(BackendConnection conn) {
		ConQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
		if (queue != null ) {
			queue.removeCon(conn);
		}
		
//		decrementTotalConnectionsSafe(); 
	}

	/**
	 * 创建新连接
	 */
	public abstract void createNewConnection(ResponseHandler handler, String schema) throws IOException;
	
	/**
	 * 测试连接，用于初始化及热更新配置检测
	 */
	public abstract boolean testConnection(String schema) throws IOException;

	public long getHeartbeatRecoveryTime() {
		return heartbeatRecoveryTime;
	}

	public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
		this.heartbeatRecoveryTime = heartbeatRecoveryTime;
	}

	public DBHostConfig getConfig() {
		return config;
	}

	public boolean isAlive() {
		return getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS;
	}
}
