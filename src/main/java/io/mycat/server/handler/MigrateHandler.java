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
import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.util.ZKUtils;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.migrate.MigrateTask;
import io.mycat.migrate.MigrateUtils;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.route.function.PartitionByCRC32PreSlot.Range;
import io.mycat.server.ServerConnection;
import io.mycat.util.ObjectUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author nange
 */
public final class MigrateHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger("MigrateHandler");

    public static void handle(String stmt, ServerConnection c) {
        Map<String, String> map = parse(stmt);

        String table = map.get("table");
        String add = map.get("add");
        if (table == null) {
            writeErrMessage(c, "table cannot be null");
            return;
        }

        if (add == null) {
            writeErrMessage(c, "add cannot be null");
            return;
        }

        try
        {
            SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(c.getSchema());
            TableConfig tableConfig = schemaConfig.getTables().get(table.toUpperCase());
            AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
            if (!(algorithm instanceof PartitionByCRC32PreSlot)) {
                writeErrMessage(c, "table: " + table + " rule is not be PartitionByCRC32PreSlot");
                return;
            }

            Map<Integer, List<Range>> integerListMap = ((PartitionByCRC32PreSlot) algorithm).getRangeMap();
            integerListMap = (Map<Integer, List<Range>>) ObjectUtil.copyObject(integerListMap);

            ArrayList<String> oldDataNodes = tableConfig.getDataNodes();
            List<String> newDataNodes = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(add);
            Map<String, List<MigrateTask>> tasks= MigrateUtils
                    .balanceExpand(table, integerListMap, oldDataNodes, newDataNodes,PartitionByCRC32PreSlot.DEFAULT_SLOTS_NUM);
             long  taskID=  System.currentTimeMillis();     //todo 需要修改唯一
            CuratorTransactionFinal transactionFinal=null;
            String taskPath = ZKUtils.getZKBasePath() + "migrate/" + table + "/" + taskID;
            CuratorFramework client= ZKUtils.getConnection();
            client.create().creatingParentsIfNeeded().forPath(taskPath);
            transactionFinal=   client.inTransaction() .setData().forPath(taskPath,stmt.getBytes("UTF-8")).and() ;
            for (Map.Entry<String, List<MigrateTask>> entry : tasks.entrySet()) {
                String key=entry.getKey();
                List<MigrateTask> value=entry.getValue();

                String path= taskPath + "/" + key;
                transactionFinal=   transactionFinal.create().forPath(path, JSON.toJSONBytes(value)).and() .setData().forPath(taskPath,stmt.getBytes("UTF-8")).and() ;
            }
            transactionFinal.commit();
        } catch (Exception e) {
            LOGGER.error("migrate error", e);
            writeErrMessage(c, "migrate error:" + e);
            return;
        }

        getOkPacket().write(c);
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
        String sql = "migrate    -table=testTable  -add=dn1,dn2,dn3  " + " \n -additional=\"a=b\"";
        Map map = parse(sql);
        System.out.println();
        for (int i = 0; i < 100; i++) {
            System.out.println(i % 5);
        }
    }

    private static Map<String, String> parse(String sql) {
        Map<String, String> map = new HashMap<>();
        List<String> rtn = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings().splitToList(sql);
        for (String s : rtn) {
            if (s.contains("=")) {
                int dindex = s.indexOf("=");
                if (s.startsWith("-")) {
                    String key = s.substring(1, dindex).trim();
                    String value = s.substring(dindex + 1).trim();
                    map.put(key, value);
                } else if (s.startsWith("--")) {
                    String key = s.substring(2, dindex).trim();
                    String value = s.substring(dindex + 1).trim();
                    map.put(key, value);
                }
            }
        }
        return map;
    }
}
