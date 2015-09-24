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

import io.mycat.MycatServer;
import io.mycat.net.BufferArray;
import io.mycat.net.NetSystem;
import io.mycat.server.Alarms;
import io.mycat.server.Fields;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.cluster.MycatNode;
import io.mycat.server.config.cluster.MycatNodeConfig;
import io.mycat.server.config.node.MycatConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.server.packet.util.PacketUtil;
import io.mycat.util.IntegerUtil;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author mycat
 */
public class ShowMyCATCluster {

    private static final Logger alarm = LoggerFactory
            .getLogger("alarm");

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("WEIGHT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void response(MySQLFrontConnection c) {
		BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
				.allocateArray();

        // write header
         header.write(bufferArray);

        // write field
        for (FieldPacket field : fields) {
            field.write(bufferArray);
        }

        // write eof
        eof.write(bufferArray);

        // write rows
        byte packetId = eof.packetId;
        for (RowDataPacket row : getRows(c)) {
            row.packetId = ++packetId;
             row.write(bufferArray);
        }

        // last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
         lastEof.write(bufferArray);

        // post write
        c.write(bufferArray);
    }

    private static List<RowDataPacket> getRows(MySQLFrontConnection c) {
        List<RowDataPacket> rows = new LinkedList<RowDataPacket>();
        MycatConfig config = MycatServer.getInstance().getConfig();
        MycatClusterConfig cluster = config.getCluster();
        Map<String, SchemaConfig> schemas = config.getSchemas();
        SchemaConfig schema = (c.getSchema() == null) ? null : schemas.get(c.getSchema());

        // 如果没有指定schema或者schema为null，则使用全部集群。
        if (schema == null) {
            Map<String, MycatNode> nodes = cluster.getNodes();
            for (MycatNode n : nodes.values()) {
                if (n != null && n.isOnline()) {
                    rows.add(getRow(n, c.getCharset()));
                }
            }
        } else {

        	 Map<String, MycatNode> nodes = cluster.getNodes();
             for (MycatNode n : nodes.values()) {
                 if (n != null && n.isOnline()) {
                     rows.add(getRow(n, c.getCharset()));
                 }
             }
        }

        if (rows.size() == 0) {
            alarm.error(Alarms.CLUSTER_EMPTY + c.toString());
        }

        return rows;
    }

    private static RowDataPacket getRow(MycatNode node, String charset) {
        MycatNodeConfig conf = node.getConfig();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(conf.getHost(), charset));
        row.add(IntegerUtil.toBytes(conf.getWeight()));
        return row;
    }

}