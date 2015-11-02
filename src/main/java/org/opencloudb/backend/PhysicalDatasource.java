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
package org.opencloudb.backend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.Alarms;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.mysql.nio.MySQLConnection;
import org.opencloudb.mysql.nio.handler.ConnectionHeartBeatHandler;
import org.opencloudb.mysql.nio.handler.DelegateResponseHandler;
import org.opencloudb.mysql.nio.handler.NewConnectionRespHandler;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.util.TimeUtil;

public abstract class PhysicalDatasource {
	private static final Logger LOGGER = Logger
			.getLogger(PhysicalDatasource.class);

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

	private boolean validSchema(String schema) {
		String theSchema = schema;
		return theSchema != null & !"".equals(theSchema)
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
                if (con.getLastTime() < hearBeatTime && heartBeatCons.size() < maxConsInOneCheck) {
                    checkListItor.remove();
                    // Heart beat check
                    con.setBorrowed(true);
                    heartBeatCons.add(con);
                }
            } else if (con.getLastTime() < hearBeatTime2) {
				    // not valid schema conntion should close for idle
					// exceed 2*conHeartBeatPeriod
					checkListItor.remove();
					con.close(" heart beate idle ");
			}

		}

	}

    public int getIndex(){
        		int currentIndex = 0;
        		for(int i=0;i<dbPool.getSources().length;i++){
            			PhysicalDatasource writeHostDatasource = dbPool.getSources()[i];
            			if(writeHostDatasource.getName().equals(getName())){
                				currentIndex = i;
                				break;
                			}
            		}
        		return currentIndex;
        	}
    	public boolean isSalveOrRead(){
        		int currentIndex = getIndex();
                if(currentIndex!=dbPool.activedIndex ||this.readNode ){
                    	return true;
                    }
                return false;
        	}

	public void heatBeatCheck(long timeout, long conHeartBeatPeriod) {
		int ildeCloseCount = hostConfig.getMinCon() * 3;
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
            closeByIdleMany(idleCons-hostConfig.getMinCon());
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
    }

    private void createByIdleLitte(int idleCons, int createCount) {
        LOGGER.info("create connections ,because idle connection not enough ,cur is "
            + idleCons + ", minCon is " + hostConfig.getMinCon() + " for " + name);
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
		if (!conn.getSchema().equals(schema)) {
			// need do schema syn in before sql send
			conn.setSchema(schema);
		}
		ConQueue queue = conMap.getSchemaConQueue(schema);
		queue.incExecuteCount();
		conn.setAttachment(attachment);
		conn.setLastTime(System.currentTimeMillis());  //每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致sql执行失败
		handler.connectionAcquired(conn);
		return conn;
	}

	private void createNewConnection(final ResponseHandler handler,
			final Object attachment, final String schema) throws IOException {
		//aysn create connection
		MycatServer.getInstance().getBusinessExecutor().execute(new Runnable() {
			public void run() {
				try {
					createNewConnection(new DelegateResponseHandler(handler) {
						@Override
						public void connectionError(Throwable e,
								BackendConnection conn) {
							handler.connectionError(e, conn);
						}

						@Override
						public void connectionAcquired(BackendConnection conn) {
							takeCon(conn, handler, attachment, schema);
						}
					}, schema);
				} catch (IOException e) {
					handler.connectionError(e, null);
				}
			}
		});
	}

    public void getConnection(String schema,boolean autocommit, final ResponseHandler handler,
        final Object attachment) throws IOException {
        BackendConnection con = this.conMap.tryTakeCon(schema,autocommit);
        if (con != null) {
            takeCon(con, handler, attachment, schema);
            return;
        } else {
            int activeCons = this.getActiveCount();//当前最大活动连接
            if(activeCons+1>size){//下一个连接大于最大连接数
                LOGGER.error("the max activeConnnections size can not be max than maxconnections");
                throw new IOException("the max activeConnnections size can not be max than maxconnections");
            }else{            // create connection
                LOGGER.info("not ilde connection in pool,create new connection for " + this.name
                        + " of schema "+schema);
                createNewConnection(handler, attachment, schema);
            }
            
        }
        
    }

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
		if (!ok) {

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
		if (queue != null) {
			queue.removeCon(conn);
		}

	}

	public abstract void createNewConnection(ResponseHandler handler,
			String schema) throws IOException;

	public long getHeartbeatRecoveryTime() {
		return heartbeatRecoveryTime;
	}

	public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
		this.heartbeatRecoveryTime = heartbeatRecoveryTime;
	}

	public DBHostConfig getConfig() {
		return config;
	}
}
