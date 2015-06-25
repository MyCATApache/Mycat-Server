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
package io.mycat.sqlengine.response;


import io.mycat.MycatServer;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net2.mysql.MySQLFrontConnection;
import io.mycat.parser.ManagerParseStop;
import io.mycat.parser.util.Pair;
import io.mycat.util.FormatUtil;
import io.mycat.util.TimeUtil;

import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 暂停数据节点心跳检测
 * 
 * @author mycat
 */
public final class StopHeartbeat {

    private static final Logger logger = Logger.getLogger(StopHeartbeat.class);

    public static void execute(String stmt, MySQLFrontConnection c) {
        int count = 0;
        Pair<String[], Integer> keys = ManagerParseStop.getPair(stmt);
        if (keys.getKey() != null && keys.getValue() != null) {
            long time = keys.getValue().intValue() * 1000L;
            Map<String, PhysicalDBPool> dns = MycatServer.getInstance().getConfig().getDataHosts();
            for (String key : keys.getKey()) {
            	PhysicalDBPool dn = dns.get(key);
                if (dn != null) {
                    dn.getSource().setHeartbeatRecoveryTime(TimeUtil.currentTimeMillis() + time);
                    ++count;
                    StringBuilder s = new StringBuilder();
                    s.append(dn.getHostName()).append(" stop heartbeat '");
                    logger.warn(s.append(FormatUtil.formatTime(time, 3)).append("' by manager."));
                }
            }
        }
        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = count;
        packet.serverStatus = 2;
        packet.write(c);
    }

}