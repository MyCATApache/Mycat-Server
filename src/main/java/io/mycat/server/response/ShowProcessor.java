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
import io.mycat.net.BufferPool;
import io.mycat.net.NetSystem;
import io.mycat.server.Fields;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.MySQLFrontConnectionNIOUtils;
import io.mycat.server.MycatServer;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.server.packet.util.PacketUtil;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;

import java.nio.ByteBuffer;

/**
 * 查看处理器状态
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowProcessor {

    private static final int FIELD_COUNT = 12;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REACT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("R_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("W_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FREE_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("BU_PERCENT", Fields.FIELD_TYPE_TINY);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BU_WARNS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("FC_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BC_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(MySQLFrontConnection c) {
    	BufferArray bufferArray = NetSystem.getInstance().getBufferPool().allocateArray();

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
        //TODO COOLLF 待修正
//        for (NIOProcessor p : MycatServer.getInstance().getProcessors()) {
//            RowDataPacket row = getRow(p, c.getCharset());
//            row.packetId = ++packetId;
//            buffer = row.write(buffer, c,true);
//        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.write(bufferArray);

        // write buffer
        c.write(bufferArray);
    }

//    private static RowDataPacket getRow(NIOProcessor processor, String charset) {
//    	BufferPool bufferPool=processor.getBufferPool();
//    	int bufferSize=bufferPool.size();
//    	int bufferCapacity=bufferPool.capacity();
//    	long bufferSharedOpts=bufferPool.getSharedOptsCount();
//    	int bufferUsagePercent=(bufferCapacity-bufferSize)*100/bufferCapacity;
//        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
//        row.add(processor.getName().getBytes());
//        row.add(LongUtil.toBytes(processor.getNetInBytes()));
//        row.add(LongUtil.toBytes(processor.getNetOutBytes()));
//        row.add(LongUtil.toBytes(0));
//        row.add(IntegerUtil.toBytes(0));
//        row.add(IntegerUtil.toBytes(processor.getWriteQueueSize()));
//        row.add(IntegerUtil.toBytes(bufferSize));
//        row.add(IntegerUtil.toBytes(bufferCapacity));
//        row.add(IntegerUtil.toBytes(bufferUsagePercent));
//        row.add(LongUtil.toBytes(bufferSharedOpts));
//        row.add(IntegerUtil.toBytes(processor.getFrontends().size()));
//        row.add(IntegerUtil.toBytes(processor.getBackends().size()));
//        return row;
//    }

}