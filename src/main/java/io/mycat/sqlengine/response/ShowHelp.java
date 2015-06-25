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
package io.mycat.sqlengine.response;

import io.mycat.config.Fields;
import io.mycat.mysql.PacketUtil;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.net2.BufferArray;
import io.mycat.net2.NetSystem;
import io.mycat.net2.mysql.MySQLFrontConnection;
import io.mycat.util.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 打印MycatServer所支持的语句
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowHelp {

	private static final int FIELD_COUNT = 2;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("STATEMENT",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("DESCRIPTION",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void execute(MySQLFrontConnection c) {
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
		for (String key : keys) {
			RowDataPacket row = getRow(key, helps.get(key), c.getCharset());
			row.packetId = ++packetId;
			row.write(bufferArray);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		lastEof.write(bufferArray);

		// post write
		c.write(bufferArray);
	}

	private static RowDataPacket getRow(String stmt, String desc, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(stmt, charset));
		row.add(StringUtil.encode(desc, charset));
		return row;
	}

	private static final Map<String, String> helps = new HashMap<String, String>();
	private static final List<String> keys = new ArrayList<String>();
	static {
		// show
		helps.put("show @@time.current", "Report current timestamp");
		helps.put("show @@time.startup", "Report startup timestamp");
		helps.put("show @@version", "Report Mycat Server version");
		helps.put("show @@server", "Report server status");
		helps.put("show @@threadpool", "Report threadPool status");
		helps.put("show @@database", "Report databases");
		helps.put("show @@datanode", "Report dataNodes");
		helps.put("show @@datanode where schema = ?", "Report dataNodes");
		helps.put("show @@datasource where dataNode = ?", "Report dataSources");
		helps.put("show @@datasource", "Report dataSources");
		helps.put("show @@processor", "Report processor status");
		helps.put("show @@command", "Report commands status");
		helps.put("show @@connection", "Report connection status");
		helps.put("show @@cache", "Report system cache usage");
		helps.put("show @@backend", "Report backend connection status");
		helps.put("show @@session", "Report front session details");
		helps.put("show @@connection.sql", "Report connection sql");
		helps.put("show @@sql.execute", "Report execute status");
		helps.put("show @@sql.detail where id = ?",
				"Report execute detail status");
		helps.put("show @@sql where id = ?", "Report specify SQL");
		helps.put("show @@sql.slow", "Report slow SQL");
		helps.put("show @@parser", "Report parser status");
		helps.put("show @@router", "Report router status");
		helps.put("show @@heartbeat", "Report heartbeat status");
		helps.put("show @@slow where schema = ?", "Report schema slow sql");
		helps.put("show @@slow where datanode = ?", "Report datanode slow sql");

		// switch
		helps.put("switch @@datasource name:index", "Switch dataSource");

		// kill
		helps.put("kill @@connection id1,id2,...",
				"Kill the specified connections");

		// stop
		helps.put("stop @@heartbeat name:time", "Pause dataNode heartbeat");

		// reload
		helps.put("reload @@config", "Reload basic config from file");
		helps.put("reload @@config_all", "Reload all config from file");
		helps.put("reload @@route", "Reload route config from file");
		helps.put("reload @@user", "Reload user config from file");

		// rollback
		helps.put("rollback @@config", "Rollback all config from memory");
		helps.put("rollback @@route", "Rollback route config from memory");
		helps.put("rollback @@user", "Rollback user config from memory");

		// offline/online
		helps.put("offline", "Change MyCat status to OFF");
		helps.put("online", "Change MyCat status to ON");

		// clear
		helps.put("clear @@slow where schema = ?", "Clear slow sql by schema");
		helps.put("clear @@slow where datanode = ?",
				"Clear slow sql by datanode");

		// list sort
		keys.addAll(helps.keySet());
		Collections.sort(keys);
	}

}