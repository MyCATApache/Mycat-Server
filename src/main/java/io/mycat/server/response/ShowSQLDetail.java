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

import io.mycat.net.BufferArray;
import io.mycat.net.NetSystem;
import io.mycat.server.Fields;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.server.packet.util.PacketUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * 查询指定SQL在各个pool中的执行情况
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowSQLDetail {

    private static final NumberFormat nf = DecimalFormat.getInstance();
    private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        nf.setMaximumFractionDigits(3);

        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("DATA_SOURCE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LAST_EXECUTE_TIMESTAMP", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(MySQLFrontConnection c, long sql) {
    	BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
				.allocateArray();

        // write header
        header.write(bufferArray);

        // write fields
        for (FieldPacket field : fields) {
           field.write(bufferArray);
        }

        // write eof
        eof.write(bufferArray);

        // write rows
        byte packetId = eof.packetId;
        for (int i = 0; i < 3; i++) {
            RowDataPacket row = getRow(sql, c.getCharset());
            row.packetId = ++packetId;
            row.write(bufferArray);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.write(bufferArray);

        // write buffer
        c.write(bufferArray);
    }

    private static RowDataPacket getRow(long sql, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add("mysql_1".getBytes());
        row.add(LongUtil.toBytes(123L));
        row.add(StringUtil.encode(nf.format(2.3), charset));
        row.add(LongUtil.toBytes(1279188420682L));
        row.add(StringUtil.encode(nf.format(3.42), charset));
        return row;
    }

}