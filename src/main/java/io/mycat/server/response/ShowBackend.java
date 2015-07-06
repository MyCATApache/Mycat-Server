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

import io.mycat.backend.BackendConnection;
import io.mycat.backend.nio.MySQLBackendConnection;
import io.mycat.net.BufferArray;
import io.mycat.net.Connection;
import io.mycat.net.NetSystem;
import io.mycat.server.Fields;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.server.packet.util.PacketUtil;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;
import io.mycat.util.TimeUtil;

/**
 * 查询后端连接
 * 
 * @author mycat
 */
public class ShowBackend {

	private static final int FIELD_COUNT = 16;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("processor",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("id", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("mysqlId", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("host", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("port", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("l_port", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("net_in", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("net_out", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("life", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("closed", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		// fields[i] = PacketUtil.getField("run", Fields.FIELD_TYPE_VAR_STRING);
		// fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("borrowed",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("schema", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil
				.getField("charset", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil
				.getField("txlevel", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("autocommit",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void execute(MySQLFrontConnection c) {
		BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
				.allocateArray();
		header.write(bufferArray);
		for (FieldPacket field : fields) {
			field.write(bufferArray);
		}
		eof.write(bufferArray);
		byte packetId = eof.packetId;
		String charset = c.getCharset();
	

			for ( Connection con : NetSystem.getInstance().getAllConnectios().values()) {
				if(con instanceof BackendConnection)
				{
					RowDataPacket row = getRow((BackendConnection) con, charset);
					row.packetId = ++packetId;
					row.write(bufferArray);
				}
			
		}
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		lastEof.write(bufferArray);
		c.write(bufferArray);
	}

	private static RowDataPacket getRow(BackendConnection c, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add("N/A".getBytes());
		long netInBytes = 0;
		long netOutbytes = 0;
		row.add(LongUtil.toBytes(c.getId()));
		long threadId = 0;
		if (c instanceof Connection) {
			Connection nioCon = (Connection) c;
			if (nioCon instanceof MySQLBackendConnection) {
				threadId = ((MySQLBackendConnection) nioCon).getThreadId();
			}

			netInBytes = nioCon.getNetInBytes();
			netOutbytes = nioCon.getNetOutBytes();
		}
		row.add(LongUtil.toBytes(threadId));
		row.add(StringUtil.encode(c.getHost(), charset));
		row.add(IntegerUtil.toBytes(c.getPort()));
		row.add(IntegerUtil.toBytes(c.getLocalPort()));
		row.add(LongUtil.toBytes(netInBytes));
		row.add(LongUtil.toBytes(netOutbytes));
		row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c
				.getStartupTime()) / 1000L));
		row.add(c.isClosed() ? "true".getBytes() : "false".getBytes());
		// boolean isRunning = c.isRunning();
		// row.add(isRunning ? "true".getBytes() : "false".getBytes());
		boolean isBorrowed = c.isBorrowed();
		row.add(isBorrowed ? "true".getBytes() : "false".getBytes());
		int writeQueueSize = 0;
		String schema = "";
		String charsetInf = "";
		String txLevel = "";
		String txAutommit = "";

		if (c instanceof MySQLBackendConnection) {
			MySQLBackendConnection mysqlC = (MySQLBackendConnection) c;
			writeQueueSize = mysqlC.getWriteQueue().size();
			schema = mysqlC.getSchema();
			charsetInf = mysqlC.getCharset() + ":" + mysqlC.getCharsetIndex();
			txLevel = mysqlC.getTxIsolation() + "";
			txAutommit = mysqlC.isAutocommit() + "";
		}
		row.add(IntegerUtil.toBytes(writeQueueSize));
		row.add(schema.getBytes());
		row.add(charsetInf.getBytes());
		row.add(txLevel.getBytes());
		row.add(txAutommit.getBytes());
		return row;
	}
}