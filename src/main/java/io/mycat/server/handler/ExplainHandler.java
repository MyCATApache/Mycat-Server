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
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.util.SchemaUtil;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public class ExplainHandler {

	private static final Logger logger = LoggerFactory.getLogger(ExplainHandler.class);
    private final static Pattern pattern = Pattern.compile("(?:(\\s*next\\s+value\\s+for\\s*MYCATSEQ_(\\w+))(,|\\)|\\s)*)+", Pattern.CASE_INSENSITIVE);
	private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[0];
	private static final int FIELD_COUNT = 2;
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	static {
		fields[0] = PacketUtil.getField("DATA_NODE",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[1] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
	}

	public static void handle(String stmt, ServerConnection c, int offset) {
		stmt = stmt.substring(offset).trim();

		RouteResultset rrs = getRouteResultset(c, stmt);
		if (rrs == null) {
			return;
		}

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

		// write rows
		RouteResultsetNode[] rrsn =  rrs.getNodes();
		for (RouteResultsetNode node : rrsn) {
			RowDataPacket row = getRow(node, c.getCharset());
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

	private static RowDataPacket getRow(RouteResultsetNode node, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(node.getName(), charset));
		row.add(StringUtil.encode(node.getStatement().replaceAll("[\\t\\n\\r]", " "), charset));
		return row;
	}

	private static RouteResultset getRouteResultset(ServerConnection c,
			String stmt) {
		String db = c.getSchema();
        int sqlType = ServerParse.parse(stmt) & 0xff;
		if (db == null) {
            db = SchemaUtil.detectDefaultDb(stmt, sqlType);

            if(db==null)
            {
                c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return null;
            }
		}
		SchemaConfig schema = MycatServer.getInstance().getConfig()
				.getSchemas().get(db);
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
					+ db + "'");
			return null;
		}
		try {

            if(ServerParse.INSERT==sqlType&&isMycatSeq(stmt, schema))
            {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "insert sql using mycat seq,you must provide primaryKey value for explain");
                return null;
            }
            SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
            return MycatServer.getInstance().getRouterservice()
					.route(system,schema, sqlType, stmt, c.getCharset(), c);
		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			logger.warn(s.append(c).append(stmt).toString()+" error:"+ e);
			String msg = e.getMessage();
			c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e
					.getClass().getSimpleName() : msg);
			return null;
		}
	}

    private static boolean isMycatSeq(String stmt, SchemaConfig schema)
    {
        if(pattern.matcher(stmt).find()) {
			return true;
		}
        SQLStatementParser parser =new MySqlStatementParser(stmt);
        MySqlInsertStatement statement = (MySqlInsertStatement) parser.parseStatement();
        String tableName=   statement.getTableName().getSimpleName();
        TableConfig tableConfig= schema.getTables().get(tableName.toUpperCase());
        if(tableConfig==null) {
			return false;
		}
        if(tableConfig.isAutoIncrement())
        {
            boolean isHasIdInSql=false;
            String primaryKey = tableConfig.getPrimaryKey();
            List<SQLExpr> columns = statement.getColumns();
            for (SQLExpr column : columns)
            {
                String columnName = column.toString();
                if(primaryKey.equalsIgnoreCase(columnName))
                {
                    isHasIdInSql = true;
                    break;
                }
            }
            if(!isHasIdInSql) {
				return true;
			}
        }


        return false;
    }

}
