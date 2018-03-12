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

import java.nio.ByteBuffer;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.FrontendConnection;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;
import io.mycat.util.TimeUtil;

/**
 * 查看当前有效连接信息
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowConnection {

	private static final int FIELD_COUNT = 15;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("PROCESSOR",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("LOCAL_PORT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil
				.getField("CHARSET", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ALIVE_TIME(S)",
				Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("RECV_BUFFER", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil
				.getField("txlevel", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("autocommit",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c) {
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
		String charset = c.getCharset();
		NIOProcessor[] processors = MycatServer.getInstance().getProcessors();
		for (NIOProcessor p : processors) {
			for (FrontendConnection fc : p.getFrontends().values()) {
				if (fc != null) {
					RowDataPacket row = getRow(fc, charset);
					row.packetId = ++packetId;
					buffer = row.write(buffer, c, true);
				}
			}
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}

	private static RowDataPacket getRow(FrontendConnection c, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(c.getProcessor().getName().getBytes());
		row.add(LongUtil.toBytes(c.getId()));
		row.add(StringUtil.encode(c.getHost(), charset));
		row.add(IntegerUtil.toBytes(c.getPort()));
		row.add(IntegerUtil.toBytes(c.getLocalPort()));
		row.add(StringUtil.encode(c.getUser(), charset));
		row.add(StringUtil.encode(c.getSchema(), charset));
		row.add(StringUtil.encode(c.getCharset()+":"+c.getCharsetIndex(), charset));
		row.add(LongUtil.toBytes(c.getNetInBytes()));
		row.add(LongUtil.toBytes(c.getNetOutBytes()));
		row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
		ByteBuffer bb = c.getReadBuffer();
		row.add(IntegerUtil.toBytes(bb == null ? 0 : bb.capacity()));
		row.add(IntegerUtil.toBytes(c.getWriteQueue().size()));

		String txLevel = "";
		String txAutommit = "";
		if (c instanceof ServerConnection) {
			ServerConnection mysqlC = (ServerConnection) c;
			txLevel = mysqlC.getTxIsolation() + "";
			txAutommit = mysqlC.isAutocommit() + "";
		}
		row.add(txLevel.getBytes());
		row.add(txAutommit.getBytes());

		return row;
	}

}