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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import io.mycat.statistic.DataSourceSyncRecorder;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;


/**
 * @author songwie
 */
public class ShowDatasourceCluster {

	private static final int FIELD_COUNT = 17;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/*private static final String[] MYSQL_CLUSTER_STAUTS_COLMS = new String[] {
	"wsrep_incoming_addresses","wsrep_cluster_size","wsrep_cluster_status", "wsrep_connected", "wsrep_flow_control_paused",
	"wsrep_local_state_comment","wsrep_ready","wsrep_flow_control_paused_ns","wsrep_flow_control_recv","wsrep_local_bf_aborts", 
	"wsrep_local_recv_queue_avg","wsrep_local_send_queue_avg","wsrep_apply_oool","wsrep_apply_oooe"};*/

	
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
		
		fields[i] = PacketUtil.getField("wsrep_incoming_addresses", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_cluster_size", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_cluster_status", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_connected", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("wsrep_flow_control_paused", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_local_state_comment", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_ready", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("wsrep_flow_control_paused_ns", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("wsrep_flow_control_recv", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("wsrep_local_bf_aborts", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("wsrep_local_recv_queue_avg", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_local_send_queue_avg", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_apply_oool", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("wsrep_apply_oooe", Fields.FIELD_TYPE_VAR_STRING);
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
		
		for (RowDataPacket row : getRows(c.getCharset())) {
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
 
	private static List<RowDataPacket> getRows(String charset) {
		List<RowDataPacket> list = new LinkedList<RowDataPacket>();
		MycatConfig conf = MycatServer.getInstance().getConfig();
		// host nodes
		Map<String, PhysicalDBPool> dataHosts = conf.getDataHosts();
		for (PhysicalDBPool pool : dataHosts.values()) {
			for (PhysicalDatasource ds : pool.getAllDataSources()) {
				DBHeartbeat hb = ds.getHeartbeat();
				DataSourceSyncRecorder record = hb.getAsynRecorder();
				Map<String, String> states = record.getRecords();
				RowDataPacket row = new RowDataPacket(FIELD_COUNT);
				if(!states.isEmpty()){
					row.add(StringUtil.encode(ds.getName(),charset));
					row.add(StringUtil.encode(ds.getConfig().getIp(),charset));
					row.add(LongUtil.toBytes(ds.getConfig().getPort()));
					
					row.add(StringUtil.encode(states.get("wsrep_incoming_addresses")==null?"":states.get("wsrep_incoming_addresses"),charset));
					row.add(StringUtil.encode(states.get("wsrep_cluster_size")==null?"":states.get("wsrep_cluster_size"),charset));
					row.add(StringUtil.encode(states.get("wsrep_cluster_status")==null?"":states.get("wsrep_cluster_status"),charset));
					row.add(StringUtil.encode(states.get("wsrep_connected")==null?"":states.get("wsrep_connected"),charset));
					row.add(StringUtil.encode(states.get("wsrep_flow_control_paused")==null?"":states.get("wsrep_flow_control_paused"),charset));
					row.add(StringUtil.encode(states.get("wsrep_local_state_comment")==null?"":states.get("wsrep_local_state_comment"),charset));
					row.add(StringUtil.encode(states.get("wsrep_ready")==null?"":states.get("wsrep_ready"),charset));
					row.add(StringUtil.encode(states.get("wsrep_flow_control_paused_ns")==null?"":states.get("wsrep_flow_control_paused_ns"),charset));
					row.add(StringUtil.encode(states.get("wsrep_flow_control_recv")==null?"":states.get("wsrep_flow_control_recv"),charset));
					row.add(StringUtil.encode(states.get("wsrep_local_bf_aborts")==null?"":states.get("wsrep_local_bf_aborts"),charset));
					row.add(StringUtil.encode(states.get("wsrep_local_recv_queue_avg")==null?"":states.get("wsrep_local_recv_queue_avg"),charset));
					row.add(StringUtil.encode(states.get("wsrep_local_send_queue_avg")==null?"":states.get("wsrep_local_recv_queue_avg"),charset));
					row.add(StringUtil.encode(states.get("wsrep_apply_oool")==null?"":states.get("wsrep_apply_oool"),charset));
					row.add(StringUtil.encode(states.get("wsrep_apply_oooe")==null?"":states.get("wsrep_apply_oooe"),charset));


					list.add(row);
				}
			}
		}
		return list;
	}

}