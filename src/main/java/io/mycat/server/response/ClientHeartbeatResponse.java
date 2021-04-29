/*
 * Copyright (c) 2020, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

/**
 * 构建select 1语句返回包：
 * 客户端连接池给mycat发送select 1或select 1 from dual心跳语句,mycat拦截，用该类直接返回包
 */
public class ClientHeartbeatResponse {
    private static byte[] RESPONSE_DATA = null;

    public static void response(ServerConnection c) {
        if (RESPONSE_DATA == null) {
            synchronized (ClientHeartbeatResponse.class) {
                if (RESPONSE_DATA == null) {
                    RESPONSE_DATA = buildPacket(c);
                }
            }
        }

        ByteBuffer buffer = c.allocate();
        buffer.put(RESPONSE_DATA);
        c.write(buffer);
    }

    private static byte[] buildPacket(ServerConnection c) {
        byte packetId = 0;
        final int FIELD_COUNT = 1;
        final String FIELD_NAME = "1";
        final String FIELD_VALUE = "1";
		ByteBuffer buffer = c.allocate();

        // write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        header.packetId = ++packetId;
		buffer = header.write(buffer, c, false);

        FieldPacket field = PacketUtil.getField(FIELD_NAME, Fields.FIELD_TYPE_VAR_STRING);
        field.packetId = ++packetId;
        buffer = field.write(buffer, c, false);

        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
		buffer = eof.write(buffer, c, false);

        // write body
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(FIELD_VALUE, c.getCharset()));
		row.packetId = ++packetId;
		buffer = row.write(buffer, c, false);

        EOFPacket rowEof = new EOFPacket();
        rowEof.packetId = ++packetId;
        buffer = rowEof.write(buffer, c, false);

        buffer.flip();
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);

        // recycle buffer
        c.recycle(buffer);
        return data;
	}

}
