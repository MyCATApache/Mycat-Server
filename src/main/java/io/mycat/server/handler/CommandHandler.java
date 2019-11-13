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
import java.sql.SQLNonTransientException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.SimpleLogHandler;
import io.mycat.backend.mysql.nio.handler.SingleNodeHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.route.RouteResultset;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.util.SchemaUtil;
import io.mycat.statistic.stat.QueryResult;
import io.mycat.statistic.stat.QueryResultDispatcher;
/**
 * <pre>
 * 用于解决mysql 协议中com_field_list命令的支持
 * https://dev.mysql.com/doc/internals/en/com-field-list.html
 * </pre>
 * @author stones_he@163.com
 */
public class CommandHandler {
	private static final Logger logger = LoggerFactory.getLogger(CommandHandler.class);
	//
	public static void handle(String stmt, ServerConnection c, int offset) {
		String table = stmt.substring(offset).trim();
		//int sqlType = ServerParse.parse(stmt) & 0xff;
		String db = c.getSchema();
		if (db == null) {
			db = SchemaUtil.detectDefaultDb(stmt, ServerParse.COMMAND);
			if (db == null) {
				c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
				return;
			}
		}
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(db);
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
			return;
		}
		SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
		RouteResultset rrs = null;
		try {
			rrs = MycatServer	.getInstance()
								.getRouterservice()
								.route(system, schema, ServerParse.COMMAND, stmt, c.getCharset(), c);
			if (rrs == null) {
				return;
			}
		} catch (SQLNonTransientException e) {
			logger.warn("", e);
			c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
		}
		//
		CommandExecResultHandler handler = new CommandExecResultHandler(rrs, c.getSession2(), table);
		try {
			handler.execute();
		} catch (Exception e1) {
			logger.warn("", e1);
			c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e1.toString());
		}
	}
}
class CommandExecResultHandler extends SingleNodeHandler {
	private static final Logger logger = LoggerFactory.getLogger(CommandExecResultHandler.class);
	private String table;
	public CommandExecResultHandler(RouteResultset rrs, NonBlockingSession session, String table) {
		super(rrs, session);
		this.table = table;
	}
	private void rowEofResponse0(byte[] eof, BackendConnection conn) {
		logger.debug("CommandExecResultHandler.rowEofResponse eof: {}", SimpleLogHandler.bytesToHex(eof));
		// 
		ServerConnection source = getSession().getSource();
		conn.recordSql(source.getHost(), source.getSchema(), getRouteResultsetNode().getStatement());
		// 判断是调用存储过程的话不能在这里释放链接
		if (!getRouteResultset().isCallStatement() || (getRouteResultset().isCallStatement() && getRouteResultset()
																													.getProcedure()
																													.isResultSimpleValue())) {
			getSession().releaseConnectionIfSafe(conn, logger.isDebugEnabled(), false);
			endRunning();
		}
		//
		int resultSize = source.getWriteQueue().size() * MycatServer.getInstance()
																	.getConfig()
																	.getSystem()
																	.getBufferPoolPageSize();
		resultSize = resultSize + getBuffer().position();
		if (!errorRepsponsed.get() && !getSession().closed() && source.canResponse()) {
			source.write(getBuffer());
		}
		source.setExecuteSql(null);
		//查询结果派发
		QueryResult queryResult = new QueryResult(	getSession().getSource().getUser(),
													getRouteResultset().getSqlType(),
													getRouteResultset().getStatement(),
													affectedRows,
													netInBytes,
													netOutBytes,
													startTime,
													System.currentTimeMillis(),
													resultSize,
													source.getHost());
		QueryResultDispatcher.dispatchQuery(queryResult);
	}
	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		logger.debug("CommandExecResultHandler.fieldEofResponse header: {}", SimpleLogHandler.bytesToHex(header));
		for (byte[] field : fields) {
			logger.debug("CommandExecResultHandler.fieldEofResponse fields: {}", SimpleLogHandler.bytesToHex(field));
		}
		logger.debug("CommandExecResultHandler.fieldEofResponse eof: {}", SimpleLogHandler.bytesToHex(eof));
		//
		fieldEofResponse0(header, fields, eof, conn);
		rowEofResponse0(eof, conn);
	}
	private void fieldEofResponse0(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		byte packetId = 0;
		this.netOutBytes += header.length;
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			this.netOutBytes += field.length;
		}
		header[3] = ++packetId;
		ServerConnection source = getSession().getSource();
		buffer = source.writeToBuffer(header, allocBuffer());
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			field[3] = ++packetId;
			// 保存field信息
			FieldPacket fieldPk = new FieldPacket();
			fieldPk.read(field);
			fieldPackets.add(fieldPk);
			buffer = source.writeToBuffer(field, buffer);
		}
		fieldCount = fieldPackets.size();
		eof[3] = ++packetId;
		this.netOutBytes += eof.length;
		buffer = source.writeToBuffer(eof, buffer);
	}
	@Override
	public String toString() {
		return "CommandExecResultHandler [node=" + getRouteResultsetNode() + ", table=" + table + "]";
	}
}
