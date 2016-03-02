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

import java.nio.ByteBuffer;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;

/**
 * 加入了offline状态推送，用于心跳语句。
 * 
 * @author mycat
 * @author mycat
 */
public class ShowMyCatStatus {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    private static final RowDataPacket status = new RowDataPacket(FIELD_COUNT);
    private static final EOFPacket lastEof = new EOFPacket();
    private static final ErrorPacket error = PacketUtil.getShutdown();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
        status.add("ON".getBytes());
        status.packetId = ++packetId;
        lastEof.packetId = ++packetId;
    }

    public static void response(ServerConnection c) {
        if (MycatServer.getInstance().isOnline()) {
            ByteBuffer buffer = c.allocate();
            buffer = header.write(buffer, c,true);
            for (FieldPacket field : fields) {
                buffer = field.write(buffer, c,true);
            }
            buffer = eof.write(buffer, c,true);
            buffer = status.write(buffer, c,true);
            buffer = lastEof.write(buffer, c,true);
            c.write(buffer);
        } else {
            error.write(c);
        }
    }

}