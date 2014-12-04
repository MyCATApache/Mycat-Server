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
package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.parser.util.Pair;
import org.opencloudb.parser.util.PairUtil;
import org.opencloudb.util.IntegerUtil;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.StringUtil;
import org.opencloudb.util.TimeUtil;

/**
 * 查看数据节点信息
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowDataNode {

	private static final NumberFormat nf = DecimalFormat.getInstance();
	private static final int FIELD_COUNT = 12;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		nf.setMaximumFractionDigits(3);

		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil
				.getField("DATHOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("INDEX", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TOTAL_TIME", Fields.FIELD_TYPE_DOUBLE);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("MAX_TIME", Fields.FIELD_TYPE_DOUBLE);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("MAX_SQL", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("RECOVERY_TIME",
				Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c, String name) {
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c, true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}

		// write eof
		buffer = eof.write(buffer, c, true);

		// write rows
		byte packetId = eof.packetId;
		MycatConfig conf = MycatServer.getInstance().getConfig();
		Map<String, PhysicalDBNode> dataNodes = conf.getDataNodes();
		List<String> keys = new ArrayList<String>();
		if (StringUtil.isEmpty(name)) {
			keys.addAll(dataNodes.keySet());
		} else {
			SchemaConfig sc = conf.getSchemas().get(name);
			if (null != sc) {
				keys.addAll(sc.getAllDataNodes());
			}
		}
		Collections.sort(keys, new Comparators<String>());
		for (String key : keys) {
			RowDataPacket row = getRow(dataNodes.get(key), c.getCharset());
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// post write
		c.write(buffer);
	}

	private static RowDataPacket getRow(PhysicalDBNode node, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(node.getName(), charset));
		row.add(StringUtil.encode(
				node.getDbPool().getHostName() + '/' + node.getDatabase(),
				charset));
		PhysicalDBPool pool = node.getDbPool();
		PhysicalDatasource ds = pool.getSource();
		if (ds != null) {
			int active = ds.getActiveCountForSchema(node.getDatabase());
			int idle = ds.getIdleCountForSchema(node.getDatabase());
			row.add(IntegerUtil.toBytes(pool.getActivedIndex()));
			row.add(StringUtil.encode(ds.getConfig().getDbType(), charset));
			row.add(IntegerUtil.toBytes(active));
			row.add(IntegerUtil.toBytes(idle));
			row.add(IntegerUtil.toBytes(ds.getSize()));
		} else {
			row.add(null);
			row.add(null);
			row.add(null);
			row.add(null);
			row.add(null);
		}
		row.add(LongUtil.toBytes(ds.getExecuteCountForSchema(node.getDatabase())));
		row.add(StringUtil.encode(nf.format(0), charset));
		row.add(StringUtil.encode(nf.format(0), charset));
		row.add(LongUtil.toBytes(0));
		long recoveryTime = pool.getSource().getHeartbeatRecoveryTime()
				- TimeUtil.currentTimeMillis();
		row.add(LongUtil.toBytes(recoveryTime > 0 ? recoveryTime / 1000L : -1L));
		return row;
	}

	private static final class Comparators<T> implements Comparator<String> {
		@Override
		public int compare(String s1, String s2) {
			Pair<String, Integer> p1 = PairUtil.splitIndex(s1, '[', ']');
			Pair<String, Integer> p2 = PairUtil.splitIndex(s2, '[', ']');
			if (p1.getKey().compareTo(p2.getKey()) == 0) {
				return p1.getValue() - p2.getValue();
			} else {
				return p1.getKey().compareTo(p2.getKey());
			}
		}
	}

}