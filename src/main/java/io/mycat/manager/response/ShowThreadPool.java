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

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.NameableExecutor;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * 查看线程池状态
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowThreadPool {

	private static final int FIELD_COUNT = 6;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("POOL_SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TASK_QUEUE_SIZE",
				Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("COMPLETED_TASK",
				Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TOTAL_TASK",
				Fields.FIELD_TYPE_LONGLONG);
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
		List<NameableExecutor> executors = getExecutors();
		for (NameableExecutor exec : executors) {
			if (exec != null) {
				RowDataPacket row = getRow(exec, c.getCharset());
				row.packetId = ++packetId;
				buffer = row.write(buffer, c, true);
			}
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}

	private static RowDataPacket getRow(NameableExecutor exec, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(exec.getName(), charset));
		row.add(IntegerUtil.toBytes(exec.getPoolSize()));
		row.add(IntegerUtil.toBytes(exec.getActiveCount()));
		row.add(IntegerUtil.toBytes(exec.getQueue().size()));
		row.add(LongUtil.toBytes(exec.getCompletedTaskCount()));
		row.add(LongUtil.toBytes(exec.getTaskCount()));
		return row;
	}

	private static List<NameableExecutor> getExecutors() {
		List<NameableExecutor> list = new LinkedList<NameableExecutor>();
		MycatServer server = MycatServer.getInstance();
		list.add(server.getTimerExecutor());
		// list.add(server.getAioExecutor());
		list.add(server.getBusinessExecutor());
		// for (NIOProcessor pros : server.getProcessors()) {
		// list.add(pros.getExecutor());
		// }
		return list;
	}
}