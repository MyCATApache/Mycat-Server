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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.Fields;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.parser.ManagerParseShow;
import org.opencloudb.statistic.DataSourceSyncRecorder;
import org.opencloudb.statistic.DataSourceSyncRecorder.Record;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.StringUtil;


/**
 * @author songwie
 */
public class ShowDatasourceSynDetail {

	private static final int FIELD_COUNT = 8;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("name", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("host", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("port", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Master_Host", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("Master_Port", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Master_Use", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DATETIME);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Seconds_Behind_Master", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		
		eof.packetId = ++packetId;
	}

	public static void response(ManagerConnection c,String stmt) {
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c,true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c,true);
		}

		// write eof
		buffer = eof.write(buffer, c,true);

		// write rows
		byte packetId = eof.packetId;
		
		String name = ManagerParseShow.getWhereParameter(stmt);
		for (RowDataPacket row : getRows(name,c.getCharset())) {
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// post write
		c.write(buffer);
	}
	 
	private static List<RowDataPacket> getRows(String name,String charset) {
		List<RowDataPacket> list = new LinkedList<RowDataPacket>();
		MycatConfig conf = MycatServer.getInstance().getConfig();
		// host nodes
		Map<String, PhysicalDBPool> dataHosts = conf.getDataHosts();
		for (PhysicalDBPool pool : dataHosts.values()) {
			for (PhysicalDatasource ds : pool.getAllDataSources()) {
				DBHeartbeat hb = ds.getHeartbeat();
				DataSourceSyncRecorder record = hb.getAsynRecorder();
				Map<String, String> states = record.getRecords();
				if(name.equals(ds.getName())){
					List<Record> data = record.getAsynRecords();
					for(Record r : data){
						RowDataPacket row = new RowDataPacket(FIELD_COUNT);

						row.add(StringUtil.encode(ds.getName(),charset));
						row.add(StringUtil.encode(ds.getConfig().getIp(),charset));
						row.add(LongUtil.toBytes(ds.getConfig().getPort()));
						row.add(StringUtil.encode(states.get("Master_Host"),charset));
						row.add(LongUtil.toBytes(Long.valueOf(states.get("Master_Port"))));
						row.add(StringUtil.encode(states.get("Master_Use"),charset));
						String time = sdf.format(new Date(r.getTime()));
						row.add(StringUtil.encode(time,charset));
						row.add(LongUtil.toBytes((Long)r.getValue()));

						list.add(row);
					}
					break;
				}

			}
		}
		return list;
	}
}
