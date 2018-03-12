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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.parser.ManagerParseHeartbeat;
import io.mycat.route.parser.util.Pair;
import io.mycat.statistic.HeartbeatRecorder;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;


/**
 * @author songwie
 */
public class ShowHeartbeatDetail {

	private static final int FIELD_COUNT = 6;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DATETIME);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_VAR_STRING);
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
		Pair<String,String> pair = ManagerParseHeartbeat.getPair(stmt);
		String name = pair.getValue();
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
		String type = "";
		String ip = "";
		int port = 0;
		DBHeartbeat hb = null;

		Map<String, PhysicalDBPool> dataHosts = conf.getDataHosts();
		for (PhysicalDBPool pool : dataHosts.values()) {
			for (PhysicalDatasource ds : pool.getAllDataSources()) {
				if(name.equals(ds.getName())){
					hb = ds.getHeartbeat();
					type = ds.getConfig().getDbType();
					ip = ds.getConfig().getIp();
					port = ds.getConfig().getPort();
					break;
				}
			}
		}
		if(hb!=null){
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Queue<HeartbeatRecorder.Record> heatbeartRecorders = hb.getRecorder().getRecordsAll();  
			for(HeartbeatRecorder.Record record : heatbeartRecorders){
				RowDataPacket row = new RowDataPacket(FIELD_COUNT);
				row.add(StringUtil.encode(name,charset));
				row.add(StringUtil.encode(type,charset));
				row.add(StringUtil.encode(ip,charset));
				row.add(IntegerUtil.toBytes(port));
				long time = record.getTime();
				String timeStr = sdf.format(new Date(time));
				row.add(StringUtil.encode(timeStr,charset));
				row.add(LongUtil.toBytes(record.getValue()));

				list.add(row);
			}
		}else{
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(null);
			row.add(null);
			row.add(null);
			row.add(null);
			row.add(null);
			row.add(null);
			list.add(row);
		}
		
		return list;
	}

}