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
import io.mycat.util.ParseUtil;

/**
 * @author mycat
 */
public class SelectLastInsertId {

	private static final String ORG_NAME = "LAST_INSERT_ID()";
	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	static {
		byte packetId = 0;
		header.packetId = ++packetId;
	}

	public static void response(MySQLFrontConnection c, String stmt,
			int aliasIndex) {
		String alias = ParseUtil.parseAlias(stmt, aliasIndex);
		if (alias == null) {
			alias = ORG_NAME;
		}

		BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
				.allocateArray();

		// write header
		header.write(bufferArray);

		// write fields
		byte packetId = header.packetId;
		FieldPacket field = PacketUtil.getField(alias, ORG_NAME,
				Fields.FIELD_TYPE_LONGLONG);
		field.packetId = ++packetId;
		field.write(bufferArray);

		// write eof
		EOFPacket eof = new EOFPacket();
		eof.packetId = ++packetId;
		eof.write(bufferArray);

		// write rows
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(LongUtil.toBytes(c.getLastInsertId()));
		row.packetId = ++packetId;
		row.write(bufferArray);

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		lastEof.write(bufferArray);

		// post write
		c.write(bufferArray);
	}

}