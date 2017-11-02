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

import io.mycat.backend.MySQLDataSource;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import io.mycat.util.TimeUtil;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
        if (heartbeat.getSource().getHostConfig().isShowSlaveSql() ) {
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
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            int balance = heartbeat.getSource().getDbPool().getBalance();
            PhysicalDatasource source = heartbeat.getSource();
            Map<String, String> resultResult = result.getResult();
            if (source.getHostConfig().isShowSlaveSql()
                    &&(source.getHostConfig().getSwitchType() == DataHostConfig.SYN_STATUS_SWITCH_DS  ||
                    PhysicalDBPool.BALANCE_NONE!=balance  )
                    )
            {

                String Slave_IO_Running =resultResult!=null? resultResult.get(
                        "Slave_IO_Running"):null;
                String Slave_SQL_Running = resultResult!=null?resultResult.get(
                        "Slave_SQL_Running"):null;
                if (Slave_IO_Running != null
                        && Slave_IO_Running.equals(Slave_SQL_Running)
                        && Slave_SQL_Running.equals("Yes")) {
                    heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
                    String Seconds_Behind_Master = resultResult.get(
                            "Seconds_Behind_Master");
                    if (null != Seconds_Behind_Master
                            && !"".equals(Seconds_Behind_Master)) {
                        heartbeat.setSlaveBehindMaster(Integer
                                .valueOf(Seconds_Behind_Master));
                    }
                } else  if(source.isSalveOrRead())
                {
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
        // 这里应该是写反了？？
        // node 的 heartbeat失败，会调用到SQLJob.doFinished， MySQLHeartBeat.setError
        // 如果超过重试次数，则调用detector.quit(); 然后到达此函数，但是 SQLJob.doFinished
        // 已经设置 finished = true，所以导致 curJob.teminate(msg) 始终无法执行
        // 无法关闭连接
//        if (curJob != null && !curJob.isFinished()) {
        if (curJob != null && curJob.isFinished()) {
            curJob.teminate(msg);
            sqlJob = null;
        }
    }

}