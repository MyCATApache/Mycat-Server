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
package io.mycat.manager.response;

import java.util.Map;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.parser.ManagerParseSwitch;
import io.mycat.route.parser.util.Pair;

/**
 * 切换数据节点的数据源
 * 
 * @author mycat
 */
public final class SwitchDataSource {

    public static void response(String stmt, ManagerConnection c) {
        int count = 0;
        Pair<String[], Integer> pair = ManagerParseSwitch.getPair(stmt);
        Map<String, PhysicalDBPool> dns = MycatServer.getInstance().getConfig().getDataHosts();
        Integer idx = pair.getValue();
        for (String key : pair.getKey()) {
        	PhysicalDBPool dn = dns.get(key);
            if (dn != null) {
                int m = dn.getActivedIndex();
                int n = (idx == null) ? dn.next(m) : idx.intValue();
                if (dn.switchSource(n, false, "MANAGER")) {
                    ++count;
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