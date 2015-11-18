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
import io.mycat.backend.PhysicalDBNode;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.net.BufferArray;
import io.mycat.net.NetSystem;
import io.mycat.server.Fields;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.MycatConfig;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.server.packet.util.PacketUtil;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 查看数据源信息
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowDataSource {

	private static final int FIELD_COUNT = 10;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("DATANODE",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("W/R", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void execute(MySQLFrontConnection c, String name) {
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
		MycatConfig conf = MycatServer.getInstance().getConfig();
		Map<String, List<PhysicalDatasource>> dataSources = new HashMap<String, List<PhysicalDatasource>>();
		if (null != name) {
			PhysicalDBNode dn = conf.getDataNodes().get(name);
			if (dn != null) {
				List<PhysicalDatasource> dslst = new LinkedList<PhysicalDatasource>();
				dslst.addAll(dn.getDbPool().getAllDataSources());
				dataSources.put(dn.getName(), dslst);
			}

		} else {
			// add all

			for (PhysicalDBNode dn : conf.getDataNodes().values()) {
				List<PhysicalDatasource> dslst = new LinkedList<PhysicalDatasource>();
				dslst.addAll(dn.getDbPool().getAllDataSources());
				dataSources.put(dn.getName(), dslst);
			}

		}

		for (Map.Entry<String, List<PhysicalDatasource>> dsEntry : dataSources
				.entrySet()) {
			String dnName = dsEntry.getKey();
			for (PhysicalDatasource ds : dsEntry.getValue()) {
				RowDataPacket row = getRow(dnName, ds, c.getCharset());
				row.packetId = ++packetId;
				row.write(bufferArray);
			}
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		lastEof.write(bufferArray);

		// post write
		c.write(bufferArray);
	}

	private static RowDataPacket getRow(String dataNode, PhysicalDatasource ds,
			String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(dataNode, charset));
		row.add(StringUtil.encode(ds.getName(), charset));
		row.add(StringUtil.encode(ds.getConfig().getDbType(), charset));
		row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
		row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
		row.add(StringUtil.encode(ds.isReadNode() ? "R" : "W", charset));
		row.add(IntegerUtil.toBytes(ds.getActiveCount()));
		row.add(IntegerUtil.toBytes(ds.getIdleCount()));
		row.add(IntegerUtil.toBytes(ds.getSize()));
		row.add(LongUtil.toBytes(ds.getExecuteCount()));
		return row;
	}

}