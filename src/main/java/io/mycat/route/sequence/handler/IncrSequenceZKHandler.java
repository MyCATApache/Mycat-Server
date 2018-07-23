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
package io.mycat.route.sequence.handler;


import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.route.util.PropertiesUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * zookeeper 实现递增序列号
 * 配置文件：sequence_conf.properties
 * 只要配置好ZK地址和表名的如下属性
 * TABLE.MINID 某线程当前区间内最小值
 * TABLE.MAXID 某线程当前区间内最大值
 * TABLE.CURID 某线程当前区间内当前值
 * 文件配置的MAXID以及MINID决定每次取得区间，这个对于每个线程或者进程都有效
 * 文件中的这三个属性配置只对第一个进程的第一个线程有效，其他线程和进程会动态读取ZK
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 23:35 2016/5/6
 */
public class IncrSequenceZKHandler extends IncrSequenceHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceHandler.class);
    private final static String PATH = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_BASE.getKey()
            + ZookeeperPath.ZK_SEPARATOR.getKey()
            + io.mycat.config.loader.zkprocess.comm.ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID)
            + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey()
            + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_INCREMENT_SEQ.getKey();
    private final static String LOCK = "/lock";
    private final static String SEQ = "/seq";
    private final static IncrSequenceZKHandler instance = new IncrSequenceZKHandler();

    public static IncrSequenceZKHandler getInstance() {
        return instance;
    }

    private ThreadLocal<Map<String, Map<String, String>>> tableParaValMapThreadLocal = new ThreadLocal<>();

    private CuratorFramework client;
    private ThreadLocal<InterProcessSemaphoreMutex> interProcessSemaphoreMutexThreadLocal = new ThreadLocal<>();
    private Properties props;

    public void load() {
        props = PropertiesUtil.loadProps(FILE_NAME);
        String zkAddress = ZkConfig.getInstance().getZkURL();
        try {
            initializeZK(props, zkAddress);
        } catch (Exception e) {
            LOGGER.error("Error caught while initializing ZK:" + e.getCause());
        }
    }

    public void threadLocalLoad() throws Exception {
        Enumeration<?> enu = props.propertyNames();
        while (enu.hasMoreElements()) {
            String key = (String) enu.nextElement();
            if (key.endsWith(KEY_MIN_NAME)) {
                handle(key);
            }
        }
    }

    public void initializeZK(Properties props, String zkAddress) throws Exception {
        this.client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
        this.client.start();
        this.props = props;
        Enumeration<?> enu = props.propertyNames();
        while (enu.hasMoreElements()) {
            String key = (String) enu.nextElement();
            if (key.endsWith(KEY_MIN_NAME)) {
                handle(key);
            }
        }
    }

    private void handle(String key) throws Exception {
        String table = key.substring(0, key.indexOf(KEY_MIN_NAME));
        InterProcessSemaphoreMutex interProcessSemaphoreMutex = interProcessSemaphoreMutexThreadLocal.get();
        if (interProcessSemaphoreMutex == null) {
            interProcessSemaphoreMutex = new InterProcessSemaphoreMutex(client, PATH + "/" + table + SEQ + LOCK);
            interProcessSemaphoreMutexThreadLocal.set(interProcessSemaphoreMutex);
        }
        Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
        if (tableParaValMap == null) {
            tableParaValMap = new HashMap<>();
            tableParaValMapThreadLocal.set(tableParaValMap);
        }
        Map<String, String> paraValMap = tableParaValMap.get(table);
        if (paraValMap == null) {
            paraValMap = new ConcurrentHashMap<>();
            tableParaValMap.put(table, paraValMap);

            String seqPath = PATH + ZookeeperPath.ZK_SEPARATOR.getKey() + table + SEQ;

            Stat stat = this.client.checkExists().forPath(seqPath);

            if (stat == null || (stat.getDataLength() == 0)) {
                paraValMap.put(table + KEY_MIN_NAME, props.getProperty(key));
                paraValMap.put(table + KEY_MAX_NAME, props.getProperty(table + KEY_MAX_NAME));
                paraValMap.put(table + KEY_CUR_NAME, props.getProperty(table + KEY_CUR_NAME));
                try {
                    String val = props.getProperty(table + KEY_MIN_NAME);
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                            .forPath(PATH + "/" + table + SEQ, val.getBytes());
                } catch (Exception e) {
                    LOGGER.debug("Node exists! Maybe other instance is initializing!");
                }
            }
            fetchNextPeriod(table);
        }
    }

    @Override
    public Map<String, String> getParaValMap(String prefixName) {
        Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
        if (tableParaValMap == null) {
            try {
                threadLocalLoad();
            } catch (Exception e) {
                LOGGER.error("Error caught while loding configuration within current thread:" + e.getCause());
            }
            tableParaValMap = tableParaValMapThreadLocal.get();
        }
        Map<String, String> paraValMap = tableParaValMap.get(prefixName);
        return paraValMap;
    }

    @Override
    public Boolean fetchNextPeriod(String prefixName) {
        InterProcessSemaphoreMutex interProcessSemaphoreMutex = interProcessSemaphoreMutexThreadLocal.get();
        try {
            if (interProcessSemaphoreMutex == null) {
                throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
            }
            interProcessSemaphoreMutex.acquire();
            Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
            if (tableParaValMap == null) {
                throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
            }
            Map<String, String> paraValMap = tableParaValMap.get(prefixName);
            if (paraValMap == null) {
                throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
            }
            if (paraValMap.get(prefixName + KEY_MAX_NAME) == null) {
                paraValMap.put(prefixName + KEY_MAX_NAME, props.getProperty(prefixName + KEY_MAX_NAME));
            }
            if (paraValMap.get(prefixName + KEY_MIN_NAME) == null) {
                paraValMap.put(prefixName + KEY_MIN_NAME, props.getProperty(prefixName + KEY_MIN_NAME));
            }
            if (paraValMap.get(prefixName + KEY_CUR_NAME) == null) {
                paraValMap.put(prefixName + KEY_CUR_NAME, props.getProperty(prefixName + KEY_CUR_NAME));
            }
            long period = Long.parseLong(paraValMap.get(prefixName + KEY_MAX_NAME))
                    - Long.parseLong(paraValMap.get(prefixName + KEY_MIN_NAME));
            long now = Long.parseLong(new String(client.getData().forPath(PATH + "/" + prefixName + SEQ)));
            client.setData().forPath(PATH + "/" + prefixName + SEQ, ((now + period + 1) + "").getBytes());

            paraValMap.put(prefixName + KEY_MAX_NAME, (now + period + 1) + "");
            paraValMap.put(prefixName + KEY_MIN_NAME, (now + 1) + "");
            paraValMap.put(prefixName + KEY_CUR_NAME, (now) + "");

        } catch (Exception e) {
            LOGGER.error("Error caught while updating period from ZK:" + e.getCause());
        } finally {
            try {
                interProcessSemaphoreMutex.release();
            } catch (Exception e) {
                LOGGER.error("Error caught while realeasing distributed lock" + e.getCause());
            }
        }
        return true;
    }

    @Override
    public Boolean updateCURIDVal(String prefixName, Long val) {
        Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
        if (tableParaValMap == null) {
            throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
        }
        Map<String, String> paraValMap = tableParaValMap.get(prefixName);
        if (paraValMap == null) {
            throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
        }
        paraValMap.put(prefixName + KEY_CUR_NAME, val + "");
        return true;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        IncrSequenceZKHandler incrSequenceZKHandler = new IncrSequenceZKHandler();
        incrSequenceZKHandler.load();
        System.out.println(incrSequenceZKHandler.nextId("TRAVELRECORD"));
        System.out.println(incrSequenceZKHandler.nextId("TRAVELRECORD"));
        System.out.println(incrSequenceZKHandler.nextId("TRAVELRECORD"));
        System.out.println(incrSequenceZKHandler.nextId("TRAVELRECORD"));
    }
}
