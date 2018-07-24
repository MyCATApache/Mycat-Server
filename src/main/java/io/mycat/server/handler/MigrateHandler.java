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
package io.mycat.server.handler;

import com.alibaba.fastjson.JSON;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.migrate.MigrateTask;
import io.mycat.migrate.MigrateTaskWatch;
import io.mycat.migrate.MigrateUtils;
import io.mycat.migrate.TaskNode;
import io.mycat.net.mysql.*;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.route.function.PartitionByCRC32PreSlot.Range;
import io.mycat.server.ServerConnection;
import io.mycat.util.ObjectUtil;
import io.mycat.util.StringUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.mycat.config.loader.zkprocess.comm.ZkParamCfg.ZK_CFG_FLAG;

/**
 * todo remove watch
 *
 * @author nange
 */
public final class MigrateHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger("MigrateHandler");

    //可以优化成多个锁
    private static final InterProcessMutex slaveIDsLock = new InterProcessMutex(ZKUtils.getConnection(), ZKUtils.getZKBasePath() + "lock/slaveIDs.lock");
    private static final int FIELD_COUNT = 1;
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static volatile boolean forceInit = false;


    static {
        fields[0] = PacketUtil.getField("TASK_ID",
                Fields.FIELD_TYPE_VAR_STRING);

    }

    private static String getUUID() {
        String s = UUID.randomUUID().toString();
        //去掉“-”符号
        return s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18) + s.substring(19, 23) + s.substring(24);
    }

    public static void handle(String stmt, ServerConnection c) {
        Map<String, String> map = parse(stmt);

        String table = map.get("table");
        String add = map.get("add");
        String timeoutString = map.get("timeout");
        String charset = map.get("charset");
        boolean forceBinlog = false;//这个命令禁止使用因为binlog stream的实现依赖mysqldump
        int timeout = 120;// minute
        String schema = "";
        if (table == null) {
            writeErrMessage(c, "table cannot be null");
            return;
        }
        if (table.contains(".")) {
            String[] split = table.split("\\.");
            schema = split[0];
            table = split[1];
        }
        if (add == null) {
            writeErrMessage(c, "add cannot be null");
            return;
        }
        if (timeoutString != null) {
            try {
                timeout = Integer.parseInt(timeoutString);
                if (timeout <= 0) {
                    throw new NumberFormatException("");
                }
            } catch (Exception e) {
                writeErrMessage(c, String.format("timeout:%s format is wrong,it should be 1-" + Integer.MAX_VALUE + " (unit:minute)", timeoutString));
                return;
            }
        }
        if (StringUtil.isEmpty(charset)) {
            charset = Charset.defaultCharset().name();
        }
        if (!Charset.isSupported(charset)) {
            writeErrMessage(c, "Not support charset " + charset);
            return;
        }
        ZkConfig zkConfig = ZkConfig.getInstance();
        boolean loadZk = "true".equalsIgnoreCase(zkConfig.getValue(ZK_CFG_FLAG));
        boolean force = "true".equalsIgnoreCase(map.get("force"));
        CuratorFramework zk = ZKUtils.getConnection();
        if (!loadZk) {
            if (!force) {
                String msg = "";
                msg += "Mycat can temporarily execute the migration command.If other mycat does not connect to this zookeeper, they will not be able to perceive changes in the migration task.\n";
                msg += "You can command as follow:\n\nmigrate -table=schema.test -add=dn2,dn3 -force=true\n\nto perform the migration.\n";
                LOGGER.error(msg);
                writeErrMessage(c, msg);
                return;
            }
            //因为loadZk在mycat启动时候没有监听zk里migrate路径，这里需要把这个监听补上
            //借用slaveIDsLock对象作为同步锁
            boolean changed = false;
            if (!forceInit) {
                synchronized (slaveIDsLock) {
                    if (!forceInit) {
                        forceInit = true;
                        changed = true;
                    }
                }
            }
            if (changed) {
                MigrateTaskWatch.start();
            }
        }
        if (zk == null) {
            writeErrMessage(c, "Mycat is not connected to zookeeper");
            return;
        }
        String taskID = getUUID();
        try {
            if (StringUtil.isEmpty(schema)) {
                schema = c.getSchema();
            }
            if (StringUtil.isEmpty(schema)) {
                writeErrMessage(c, "No database selected");
                return;
            }
            SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schema);
            if (schemaConfig == null) {
                writeErrMessage(c, String.format("Unknown database '" + schema + "'", table.toUpperCase(), schema));
                return;
            }
            TableConfig tableConfig = schemaConfig.getTables().get(table.toUpperCase());
            if (tableConfig == null) {
                writeErrMessage(c, String.format("Table '%s' doesn't define in schema '%s'\n", table.toUpperCase(), schema));
                return;
            }
            AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
            if (!(algorithm instanceof PartitionByCRC32PreSlot)) {
                writeErrMessage(c, "table: " + table + " rule is not be PartitionByCRC32PreSlot");
                return;
            }
            Map<Integer, List<Range>> integerListMap = ((PartitionByCRC32PreSlot) algorithm).getRangeMap();
            integerListMap = (Map<Integer, List<Range>>) ObjectUtil.copyObject(integerListMap);

            ArrayList<String> oldDataNodes = tableConfig.getDataNodes();
            Map<String, PhysicalDBNode> allDataNodes = MycatServer.getInstance().getConfig().getDataNodes();
            List<String> newDataNodes = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(add);
            for (String newDataNode : newDataNodes) {
                if (tableConfig.getDataNodes().contains(newDataNode)) {
                    writeErrMessage(c, "The dataNode " + newDataNode+" that needs to be added already exists\n");
                    return;
                }
                if(!allDataNodes.containsKey(newDataNode)){
                    writeErrMessage(c, "The dataNode " + newDataNode+" does not exist\n");
                    return;
                }
            }

            Map<String, List<MigrateTask>> tasks = MigrateUtils
                    .balanceExpand(table, integerListMap, oldDataNodes, newDataNodes, PartitionByCRC32PreSlot.DEFAULT_SLOTS_NUM);

            CuratorTransactionFinal transactionFinal = null;
            String taskBase = ZKUtils.getZKBasePath() + "migrate/" + schema;
            String taskPath = taskBase + "/" + taskID;
            CuratorFramework client = ZKUtils.getConnection();

            //校验 之前同一个表的迁移任务未完成，则jzhi禁止继续
            if (client.checkExists().forPath(taskBase) != null) {
                List<String> childTaskList = client.getChildren().forPath(taskBase);
                for (String child : childTaskList) {
                    String path = taskBase + "/" + child;
                    String str = new String(ZKUtils.getConnection().getData().forPath(path));
                    if (!isJson(str)) {
                        writeErrMessage(c, path + "in zookeeper is abnormal state,please repair manual!");
                        return;
                    }
                    TaskNode taskNode = JSON
                            .parseObject(str, TaskNode.class);
                    if (taskNode.getSchema().equalsIgnoreCase(schema) && table.equalsIgnoreCase(taskNode.getTable())
                            && taskNode.getStatus() < 5) {
                        writeErrMessage(c, "table: " + table + " previous migrate task is still running,on the same time one table only one task");
                        return;
                    }
                }
            }
            String backupPath = backup();
            client.create().creatingParentsIfNeeded().forPath(taskPath);
            TaskNode taskNode = new TaskNode();
            taskNode.setSchema(schema);
            taskNode.setSql(stmt);
            taskNode.setTable(table);
            taskNode.setAdd(add);
            taskNode.setStatus(0);
            taskNode.setTimeout(timeout);
            taskNode.setCharset(charset);
            taskNode.setForceBinlog(forceBinlog);
            taskNode.setBackupFile(backupPath);

            Map<String, Integer> fromNodeSlaveIdMap = new HashMap<>();

            List<MigrateTask> allTaskList = new ArrayList<>();
            for (Map.Entry<String, List<MigrateTask>> entry : tasks.entrySet()) {
                String key = entry.getKey();
                List<MigrateTask> value = entry.getValue();
                for (MigrateTask migrateTask : value) {
                    migrateTask.setSchema(schema);

                    //分配slaveid只需要一个dataHost分配一个即可，后续任务执行模拟从节点只需要一个dataHost一个
                    String dataHost = getDataHostNameFromNode(migrateTask.getFrom());
                    if (fromNodeSlaveIdMap.containsKey(dataHost)) {
                        migrateTask.setSlaveId(fromNodeSlaveIdMap.get(dataHost));
                    } else {
                        migrateTask.setSlaveId(getSlaveIdFromZKForDataNode(migrateTask.getFrom()));
                        fromNodeSlaveIdMap.put(dataHost, migrateTask.getSlaveId());
                    }

                }
                allTaskList.addAll(value);

            }


            transactionFinal = client.inTransaction().setData().forPath(taskPath, JSON.toJSONBytes(taskNode)).and();


            //合并成dataHost级别任务
            Map<String, List<MigrateTask>> dataHostMigrateMap = mergerTaskForDataHost(allTaskList);

            String boosterDataHosts = ZkConfig.getInstance().getValue(ZkParamCfg.MYCAT_BOOSTER_DATAHOSTS);
            Set<String> dataNodes = new HashSet<>(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(boosterDataHosts));
            boolean isFirst = true;
            for (String s : dataHostMigrateMap.keySet()) {
                if (!dataNodes.contains(s)) {
                    if (isFirst) {
                        LOGGER.warn("--------------------------------check dataNode--------------------------------");
                        isFirst = false;
                    }
                    LOGGER.warn("dataNode %s will be not participate in migration");
                }
            }

            for (Map.Entry<String, List<MigrateTask>> entry : dataHostMigrateMap.entrySet()) {
                String key = entry.getKey();
                List<MigrateTask> value = entry.getValue();
                String path = taskPath + "/" + key;
                transactionFinal = transactionFinal.create().forPath(path, JSON.toJSONBytes(value)).and();
            }
            transactionFinal.commit();
        } catch (Exception e) {
            LOGGER.error("migrate error", e);
            writeErrMessage(c, "migrate error:" + e);
            return;
        }

        writePackToClient(c, taskID);
        LOGGER.info("--------------------------------task created success--------------------------------");
        LOGGER.info("task start", new Date());
    }

    private static void writePackToClient(ServerConnection c, String taskID) {
        ByteBuffer buffer = c.allocate();

        // write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        byte packetId = header.packetId;
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            field.packetId = ++packetId;
            buffer = field.write(buffer, c, true);
        }

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        buffer = eof.write(buffer, c, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(taskID, c.getCharset()));
        row.packetId = ++packetId;
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }


    private static String getDataHostNameFromNode(String dataNode) {
        return MycatServer.getInstance().getConfig().getDataNodes().get(dataNode).getDbPool().getHostName();
    }

    private static Map<String, List<MigrateTask>> mergerTaskForDataHost(List<MigrateTask> migrateTaskList) {
        Map<String, List<MigrateTask>> taskMap = new HashMap<>();
        for (MigrateTask migrateTask : migrateTaskList) {
            String dataHost = getDataHostNameFromNode(migrateTask.getFrom());
            if (taskMap.containsKey(dataHost)) {
                taskMap.get(dataHost).add(migrateTask);
            } else {
                taskMap.put(dataHost, Lists.newArrayList(migrateTask));
            }
        }


        return taskMap;
    }

    private static int getSlaveIdFromZKForDataNode(String dataNode) {
        PhysicalDBNode dbNode = MycatServer.getInstance().getConfig().getDataNodes().get(dataNode);
        String slaveIDs = dbNode.getDbPool().getSlaveIDs();
        if (Strings.isNullOrEmpty(slaveIDs))
            throw new RuntimeException("dataHost:" + dbNode.getDbPool().getHostName() + " do not config the salveIDs field");

        List<Integer> allSlaveIDList = parseSlaveIDs(slaveIDs);

        String taskPath = ZKUtils.getZKBasePath() + "slaveIDs/" + dbNode.getDbPool().getHostName();
        try {
            slaveIDsLock.acquire(30, TimeUnit.SECONDS);
            Set<Integer> zkSlaveIdsSet = new HashSet<>();
            if (ZKUtils.getConnection().checkExists().forPath(taskPath) != null) {
                List<String> zkHasSlaveIDs = ZKUtils.getConnection().getChildren().forPath(taskPath);
                for (String zkHasSlaveID : zkHasSlaveIDs) {
                    zkSlaveIdsSet.add(Integer.parseInt(zkHasSlaveID));
                }
            }
            for (Integer integer : allSlaveIDList) {
                if (!zkSlaveIdsSet.contains(integer)) {
                    ZKUtils.getConnection().create().creatingParentsIfNeeded().forPath(taskPath + "/" + integer);
                    return integer;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                slaveIDsLock.release();
            } catch (Exception e) {
                LOGGER.error("error:", e);
            }
        }

        throw new RuntimeException("cannot get the slaveID  for dataHost :" + dbNode.getDbPool().getHostName());
    }

    private static List<Integer> parseSlaveIDs(String slaveIDs) {
        List<Integer> allSlaveList = new ArrayList<>();
        List<String> stringList = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(slaveIDs);
        for (String id : stringList) {
            if (id.contains("-")) {
                List<String> idRangeList = Splitter.on("-").omitEmptyStrings().trimResults().splitToList(id);
                if (idRangeList.size() != 2)
                    throw new RuntimeException(id + "slaveIds range must be 2  size");
                for (int i = Integer.parseInt(idRangeList.get(0)); i <= Integer.parseInt(idRangeList.get(1)); i++) {
                    allSlaveList.add(i);
                }

            } else {
                allSlaveList.add(Integer.parseInt(id));
            }
        }
        return allSlaveList;
    }


    private static OkPacket getOkPacket() {
        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = 0;
        packet.serverStatus = 2;
        return packet;
    }

    public static void writeErrMessage(ServerConnection c, String msg) {
        c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
    }

    public static void main(String[] args) {
        String sql = "migrate    -table=test  -add=dn2,dn3,dn4  " + " \n -additional=\"a=b\"";
        Map map = parse(sql);
        System.out.println();
        for (int i = 0; i < 100; i++) {
            System.out.println(i % 5);
        }

        TaskNode taskNode = new TaskNode();
        taskNode.setSql(sql);


        System.out.println(new String(JSON.toJSONBytes(taskNode)));
    }

    private static Map<String, String> parse(String sql) {
        Map<String, String> map = new HashMap<>();
        List<String> rtn = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings().splitToList(sql);
        for (String s : rtn) {
            if (s.contains("=")) {
                int dindex = s.indexOf("=");
                if (s.startsWith("-")) {
                    String key = s.substring(1, dindex).toLowerCase().trim();
                    String value = s.substring(dindex + 1).trim();
                    map.put(key, value);
                } else if (s.startsWith("--")) {
                    String key = s.substring(2, dindex).toLowerCase().trim();
                    String value = s.substring(dindex + 1).trim();
                    map.put(key, value);
                }
            }
        }
        return map;
    }

    public static String backup() throws Exception {
        LocalDateTime now = LocalDateTime.now();
        Path path = Paths.get(SystemConfig.getHomePath()).resolve("backup_" + now.getYear() + "_" + now.getMonthOfYear() + "_" + now.getDayOfMonth() + "_" + now.getHourOfDay() + "_" + now.getMinuteOfHour());
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
        List<String> strings = ZKUtils.getConnection().getChildren().forPath(ZKUtils.getZKBasePath() + "ruledata");
        for (String s : strings) {
            byte[] bytes = ZKUtils.getConnection().getData().forPath(ZKUtils.getZKBasePath() + "ruledata/" + s);
            Files.write(path.resolve(s), bytes);
        }
        byte[] bytes = ZKUtils.getConnection().getData().forPath(ZKUtils.getZKBasePath() + "schema/schema");
        Files.write(path.resolve("schema.json"), bytes);
        bytes = ZKUtils.getConnection().getData().forPath(ZKUtils.getZKBasePath() + "rules/function");
        Files.write(path.resolve("function.json"), bytes);
        return path.toAbsolutePath().toString();
    }

    private static boolean isJson(String str) {
        if (StringUtil.isEmpty(str)) return false;
        str = str.trim();
        if (str.startsWith("{") && str.endsWith("}")) return true;
        return false;
    }
}
