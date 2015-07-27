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
package io.mycat.backend;

import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.server.Alarms;
import io.mycat.server.MycatServer;
import io.mycat.server.config.DataHostConfig;
import io.mycat.server.executors.GetConnectionHandler;
import io.mycat.server.executors.ResponseHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class PhysicalDBPool {
    public static final int BALANCE_NONE = 0;
    public static final int BALANCE_ALL_BACK = 1;
    public static final int BALANCE_ALL = 2;
    public static final int BALANCE_ALL_READ = 3;
    public static final int WRITE_ONLYONE_NODE = 0;
    public static final int WRITE_RANDOM_NODE = 1;
    public static final int WRITE_ALL_NODE = 2;
    public static final long LONG_TIME = 300000;

    protected static final Logger LOGGER = Logger
            .getLogger(PhysicalDBPool.class);
    private final String hostName;
    protected PhysicalDatasource[] writeSources;
    protected Map<Integer, PhysicalDatasource[]> readSources;
    protected volatile int activedIndex;
    protected volatile boolean initSuccess;
    protected final ReentrantLock switchLock = new ReentrantLock();
    private final Collection<PhysicalDatasource> allDs;
    private final int banlance;
    private final int writeType;
    private final Random random = new Random();
    private final Random wnrandom = new Random();
    private String[] schemas;
    private final DataHostConfig dataHostConfig;

    public PhysicalDBPool(String name, DataHostConfig conf,
                          PhysicalDatasource[] writeSources,
                          Map<Integer, PhysicalDatasource[]> readSources, int balance,
                          int writeType) {
        this.hostName = name;
        this.dataHostConfig = conf;
        this.writeSources = writeSources;
        this.banlance = balance;
        this.writeType = writeType;
        Iterator<Map.Entry<Integer, PhysicalDatasource[]>> entryItor = readSources
                .entrySet().iterator();
        while (entryItor.hasNext()) {
            PhysicalDatasource[] values = entryItor.next().getValue();
            if (values.length == 0) {
                entryItor.remove();
            }
        }
        this.readSources = readSources;
        this.allDs = this.genAllDataSources();
        LOGGER.info("total resouces of dataHost " + this.hostName + " is :"
                + allDs.size());
        setDataSourceProps();
    }

    public int getWriteType() {
        return writeType;
    }
    public int getBalance() {
        return banlance;
    }
    private void setDataSourceProps() {
        for (PhysicalDatasource ds : this.allDs) {
            ds.setDbPool(this);
        }
    }

    public PhysicalDatasource findDatasouce(BackendConnection exitsCon) {

        for (PhysicalDatasource ds : this.allDs) {
            if (ds.isReadNode() == exitsCon.isFromSlaveDB()) {
                if (ds.isMyConnection(exitsCon)) {
                    return ds;
                }
            }
        }
        LOGGER.warn("can't find connection in pool " + this.hostName + " con:"
                + exitsCon);
        return null;
    }

    public String getHostName() {
        return hostName;
    }

    /**
     * all write datanodes
     *
     * @return
     */
    public PhysicalDatasource[] getSources() {
        return writeSources;
    }

    public PhysicalDatasource getSource() {
        switch (writeType) {
            case WRITE_ONLYONE_NODE: {
                return writeSources[activedIndex];
            }
            case WRITE_RANDOM_NODE: {

                int index = Math.abs(wnrandom.nextInt()) % writeSources.length;
                PhysicalDatasource result = writeSources[index];
                if (!this.isAlive(result)) {
                    // find all live nodes
                    ArrayList<Integer> alives = new ArrayList<Integer>(
                            writeSources.length - 1);
                    for (int i = 0; i < writeSources.length; i++) {
                        if (i != index) {
                            if (this.isAlive(writeSources[i])) {
                                alives.add(i);
                            }
                        }
                    }
                    if (alives.isEmpty()) {
                        result = writeSources[0];
                    } else {
                        // random select one
                        index = Math.abs(wnrandom.nextInt()) % alives.size();
                        result = writeSources[alives.get(index)];

                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("select write source " + result.getName()
                            + " for dataHost:" + this.getHostName());
                }
                return result;
            }
            default: {
                throw new java.lang.IllegalArgumentException("writeType is "
                        + writeType + " ,so can't return one write datasource ");
            }
        }

    }

    public int getActivedIndex() {
        return activedIndex;
    }

    public boolean isInitSuccess() {
        return initSuccess;
    }

    public int next(int i) {
        if (checkIndex(i)) {
            return (++i == writeSources.length) ? 0 : i;
        } else {
            return 0;
        }
    }

    /**
     * 鍒囨崲鏁版嵁婧�
     */
    public boolean switchSource(int newIndex, boolean isAlarm, String reason) {
        if (this.writeType != PhysicalDBPool.WRITE_ONLYONE_NODE
                || !checkIndex(newIndex)) {
            return false;
        }
        final ReentrantLock lock = this.switchLock;
        lock.lock();
        try {
            int current = activedIndex;
            if (current != newIndex) {
                // switch index
                activedIndex = newIndex;
                // init again
                this.init(activedIndex);
                // clear all connections
                this.getSources()[current].clearCons("switch datasource");
                // write log
                LOGGER.warn(switchMessage(current, newIndex, false, reason));
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    private String switchMessage(int current, int newIndex, boolean alarm,
                                 String reason) {
        StringBuilder s = new StringBuilder();
        if (alarm) {
            s.append(Alarms.DATANODE_SWITCH);
        }
        s.append("[Host=").append(hostName).append(",result=[").append(current)
                .append("->");
        s.append(newIndex).append("],reason=").append(reason).append(']');
        return s.toString();
    }

    private int loop(int i) {
        return i < writeSources.length ? i : (i - writeSources.length);
    }

    public void init(int index) {
        if (!checkIndex(index)) {
            index = 0;
        }
        int active = -1;
        for (int i = 0; i < writeSources.length; i++) {
            int j = loop(i + index);
            if (initSource(j, writeSources[j])) {
                active = j;
                activedIndex = active;
                initSuccess = true;
                LOGGER.info(getMessage(active, " init success"));

                if (this.writeType == WRITE_ONLYONE_NODE) {
                    // only init one write datasource
                    MycatServer.getInstance().saveDataHostIndex(hostName,
                            activedIndex);
                    break;
                }
            }
        }
        if (!checkIndex(active)) {
            initSuccess = false;
            StringBuilder s = new StringBuilder();
            s.append(Alarms.DEFAULT).append(hostName).append(" init failure");
            LOGGER.error(s.toString());
        }
    }

    private boolean checkIndex(int i) {
        return i >= 0 && i < writeSources.length;
    }

    private String getMessage(int index, String info) {
        return new StringBuilder().append(hostName).append(" index:")
                .append(index).append(info).toString();
    }

    private boolean initSource(int index, PhysicalDatasource ds) {
        int initSize = ds.getConfig().getMinCon();
        LOGGER.info("init backend myqsl source ,create connections total "
                + initSize + " for " + ds.getName() + " index :" + index);
        CopyOnWriteArrayList<BackendConnection> list = new CopyOnWriteArrayList<BackendConnection>();
        GetConnectionHandler getConHandler = new GetConnectionHandler(list,
                initSize);
        // long start=System.currentTimeMillis();
        // long timeOut=start+5000*1000L;

        for (int i = 0; i < initSize; i++) {
            try {

                ds.getConnection(this.schemas[i % schemas.length], true,
                        getConHandler, null);
            } catch (Exception e) {
                LOGGER.warn(getMessage(index, " init connection error."), e);
            }
        }
        long timeOut = System.currentTimeMillis() + 60 * 1000;

        // waiting for finish
        while (!getConHandler.finished()
                && (System.currentTimeMillis() < timeOut)) {
            try {
                Thread.sleep(200);

            } catch (InterruptedException e) {
                LOGGER.error("initError", e);
            }
        }
        LOGGER.info("init result :" + getConHandler.getStatusInfo());
        for (BackendConnection c : list) {
            c.release();
        }
        return !list.isEmpty();
    }

    public void doHeartbeat() {

        // 妫�煡鍐呴儴鏄惁鏈夎繛鎺ユ睜閰嶇疆淇℃伅
        if (writeSources == null || writeSources.length == 0) {
            return;
        }

        for (PhysicalDatasource source : this.allDs) {
            // 鍑嗗鎵ц蹇冭烦妫�祴
            if (source != null) {
                source.doHeartbeat();
            } else {
                StringBuilder s = new StringBuilder();
                s.append(Alarms.DEFAULT).append(hostName)
                        .append(" current dataSource is null!");
                LOGGER.error(s.toString());
            }
        }
        // 璇诲簱鐨勫績璺虫娴�
    }

    /**
     * back physical connection heartbeat check
     */
    public void heartbeatCheck(long ildCheckPeriod) {
        for (PhysicalDatasource ds : allDs) {
            // only readnode or all write node or writetype=WRITE_ONLYONE_NODE
            // and current write node will check
            if (ds != null
                    && (ds.getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS)
                    && (ds.isReadNode()
                    || (this.writeType != WRITE_ONLYONE_NODE) || (this.writeType == WRITE_ONLYONE_NODE && ds == this
                    .getSource()))) {
                ds.heatBeatCheck(ds.getConfig().getIdleTimeout(),
                        ildCheckPeriod);
            }
        }
    }

    public void startHeartbeat() {
        for (PhysicalDatasource source : this.allDs) {
            source.startHeartbeat();
        }
    }

    public void stopHeartbeat() {
        for (PhysicalDatasource source : this.allDs) {
            source.stopHeartbeat();
        }
    }

    public void clearDataSources(String reason) {
        LOGGER.info("clear datasours of pool " + this.hostName);
        for (PhysicalDatasource source : this.allDs) {
            LOGGER.info("clear datasoure of pool  " + this.hostName + " ds:"
                    + source.getConfig());
            source.clearCons(reason);
            source.stopHeartbeat();
        }

    }

    public Collection<PhysicalDatasource> genAllDataSources() {
        LinkedList<PhysicalDatasource> allSources = new LinkedList<PhysicalDatasource>();
        for (PhysicalDatasource ds : writeSources) {
            if (ds != null) {
                allSources.add(ds);
            }
        }
        for (PhysicalDatasource[] dataSources : this.readSources.values()) {
            for (PhysicalDatasource ds : dataSources) {
                if (ds != null) {
                    allSources.add(ds);
                }
            }
        }
        return allSources;
    }

    public Collection<PhysicalDatasource> getAllDataSources() {
        return this.allDs;
    }

    /**
     * return connection for read balance
     *
     * @param handler
     * @param attachment
     * @param database
     * @throws Exception
     */
    public void getRWBanlanceCon(String schema, boolean autocommit,
                                 ResponseHandler handler, Object attachment, String database)
            throws Exception {
        PhysicalDatasource theNode = null;
        ArrayList<PhysicalDatasource> okSources = null;
        switch (banlance) {
            case BALANCE_ALL_BACK: {// all read nodes and the standard by masters

                okSources = getAllActiveRWSources(false, checkSlaveSynStatus());
                if (okSources.isEmpty()) {
                    theNode = this.getSource();
                } else {
                    theNode = randomSelect(okSources);
                }
                break;
            }
            case BALANCE_ALL: {
                okSources = getAllActiveRWSources(true, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_ALL_READ: {
                okSources = getAllActiveReadSources();
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_NONE:
            default:
                // return default write data source
                theNode = this.getSource();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select read source " + theNode.getName()
                    + " for dataHost:" + this.getHostName());
        }
        theNode.getConnection(schema, autocommit, handler, attachment);
    }

    private boolean checkSlaveSynStatus() {
        return (dataHostConfig.getSlaveThreshold() != -1)
                && (dataHostConfig.getSwitchType() == DataHostConfig.SYN_STATUS_SWITCH_DS);
    }

    private PhysicalDatasource randomSelect(
            ArrayList<PhysicalDatasource> okSources) {
        if (okSources.isEmpty()) {
            return this.getSource();
        } else {
            int index = Math.abs(random.nextInt()) % okSources.size();
            return okSources.get(index);
        }

    }

    private boolean isAlive(PhysicalDatasource theSource) {
        return (theSource.getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS);
    }

    private boolean canSelectAsReadNode(PhysicalDatasource theSource) {

        if(theSource.getHeartbeat().getSlaveBehindMaster()==null
                ||theSource.getHeartbeat().getDbSynStatus()==DBHeartbeat.DB_SYN_ERROR){
            return false;
        }
        return (theSource.getHeartbeat().getDbSynStatus() == DBHeartbeat.DB_SYN_NORMAL)
                && (theSource.getHeartbeat().getSlaveBehindMaster() < this.dataHostConfig
                .getSlaveThreshold());

    }

    /**
     * return all backup write sources
     *
     * @return
     */
    private ArrayList<PhysicalDatasource> getAllActiveRWSources(
            boolean includeCurWriteNode, boolean filterWithSlaveThreshold) {
        int curActive = activedIndex;
        ArrayList<PhysicalDatasource> okSources = new ArrayList<PhysicalDatasource>(
                this.allDs.size());
        for (int i = 0; i < this.writeSources.length; i++) {
            PhysicalDatasource theSource = writeSources[i];
            if (isAlive(theSource)) {// write node is active
                if (i == curActive && includeCurWriteNode == false) {
                    // not include cur active source
                } else if (filterWithSlaveThreshold) {
                    if (canSelectAsReadNode(theSource)) {
                        okSources.add(theSource);
                    } else {
                        continue;
                    }
                } else {
                    okSources.add(theSource);
                }
                if (!readSources.isEmpty()) {
                    // check all slave nodes
                    PhysicalDatasource[] allSlaves = this.readSources.get(i);
                    if (allSlaves != null) {
                        for (PhysicalDatasource slave : allSlaves) {
                            if (isAlive(slave)) {
                                if (filterWithSlaveThreshold) {
                                    if (canSelectAsReadNode(slave)) {
                                        okSources.add(slave);
                                    } else {
                                        continue;
                                    }
                                } else {
                                    okSources.add(slave);
                                }
                            }
                        }
                    }
                }
            }

        }
        return okSources;
    }
    /**
     * return all read sources
     *
     * @return
     */
    private ArrayList<PhysicalDatasource> getAllActiveReadSources(){
        ArrayList<PhysicalDatasource> okSources = new ArrayList<PhysicalDatasource>(
                this.allDs.size());
        for (int i = 0; i < this.writeSources.length; i++) {
            if (isAlive(writeSources[i])) {// write node is active

                if (!readSources.isEmpty()) {
                    // check all slave nodes
                    PhysicalDatasource[] allSlaves = this.readSources.get(i);
                    if (allSlaves != null) {
                        for (PhysicalDatasource slave : allSlaves) {
                            if (isAlive(slave)) {
                                okSources.add(slave);
                            }
                        }
                    }
                }
            }

        }
        return okSources;
    }

    public String[] getSchemas() {
        return schemas;
    }

    public void setSchemas(String[] mySchemas) {
        this.schemas = mySchemas;
    }

}