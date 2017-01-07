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
package io.mycat.server.handler;

import java.nio.ByteBuffer;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.backend.mysql.nio.handler.SingleNodeHandler;
import io.mycat.config.Fields;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.StringUtil;

/**
 * @author rainbow
 */
public class Explain2Handler {

	private static final Logger logger = LoggerFactory.getLogger(Explain2Handler.class);
	private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[1];
	private static final int FIELD_COUNT = 2;
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	static {
		fields[0] = PacketUtil.getField("SQL",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[1] = PacketUtil.getField("MSG",
				Fields.FIELD_TYPE_VAR_STRING);
	}

	public static void handle(String stmt, ServerConnection c, int offset) {

		try {
			stmt = stmt.substring(offset);
			if(!stmt.toLowerCase().contains("datanode=") || !stmt.toLowerCase().contains("sql=")){
				showerror(stmt, c, "explain2 datanode=? sql=?");
				return ;
			}
			String dataNode = stmt.substring(stmt.indexOf("=") + 1 ,stmt.indexOf("sql=")).trim();
			String sql = "explain " + stmt.substring(stmt.indexOf("sql=") + 4 ,stmt.length()).trim();
			
			if(dataNode == null || dataNode.isEmpty() || sql == null || sql.isEmpty()){
				showerror(stmt, c, "dataNode or sql is null or empty");
				return;
			}
			
			RouteResultsetNode node = new RouteResultsetNode(dataNode, ServerParse.SELECT, sql);
			RouteResultset	rrs =  new RouteResultset(sql, ServerParse.SELECT);
			node.setSource(rrs);
			EMPTY_ARRAY[0] = node; 
			rrs.setNodes(EMPTY_ARRAY);
			SingleNodeHandler singleNodeHandler = new SingleNodeHandler(rrs, c.getSession2());
			singleNodeHandler.execute();
		} catch (Exception e) {
			logger.error(e.getMessage(), e.getCause());
			showerror(stmt, c, e.getMessage());
		}
	}
	
	private static void showerror(String stmt, ServerConnection c, String msg){
		ByteBuffer buffer = c.allocate();
		// write header
		ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
		byte packetId = header.packetId;
		buffer = header.write(buffer, c,true);

		// write fields
		for (FieldPacket field : fields) {
			field.packetId = ++packetId;
			buffer = field.write(buffer, c,true);
		}

		// write eof
		EOFPacket eof = new EOFPacket();
		eof.packetId = ++packetId;
		buffer = eof.write(buffer, c,true);

	
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(stmt, c.getCharset()));
		row.add(StringUtil.encode(msg, c.getCharset()));
		row.packetId = ++packetId;
		buffer = row.write(buffer, c,true);

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// post write
		c.write(buffer);
	}
}
