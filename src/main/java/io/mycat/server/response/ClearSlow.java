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
package io.mycat.server.response;

import static io.mycat.server.MySQLFrontConnectionNIOUtils.allocate;
import static io.mycat.server.MySQLFrontConnectionNIOUtils.writeToBuffer;
import io.mycat.MycatConfig;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.server.ErrorCode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.MycatServer;
import io.mycat.server.config.SchemaConfig;
import io.mycat.server.packet.OkPacket;

/**
 * @author mycat
 */
public class ClearSlow {

    public static void dataNode(MySQLFrontConnection c, String name) {
    	PhysicalDBNode dn = MycatServer.getInstance().getConfig().getDataNodes().get(name);
    	PhysicalDBPool ds = null;
        if (dn != null && ((ds = dn.getDbPool())!= null)) {
           // ds.getSqlRecorder().clear();
           c.write(writeToBuffer(OkPacket.OK, allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "Invalid DataNode:" + name);
        }
    }

    public static void schema(MySQLFrontConnection c, String name) {
        MycatConfig conf = MycatServer.getInstance().getConfig();
        SchemaConfig schema = conf.getSchemas().get(name);
        if (schema != null) {
//            Map<String, MySQLDataNode> dataNodes = conf.getDataNodes();
//            for (String n : schema.getAllDataNodes()) {
//                MySQLDataNode dn = dataNodes.get(n);
//                MySQLDataSource ds = null;
//                if (dn != null && (ds = dn.getSource()) != null) {
//                    ds.getSqlRecorder().clear();
//                }
//            }
           c.write(writeToBuffer(OkPacket.OK, allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "Invalid Schema:" + name);
        }
    }



}