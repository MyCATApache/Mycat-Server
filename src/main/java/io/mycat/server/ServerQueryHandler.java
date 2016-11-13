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
package io.mycat.server;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.config.ErrorCode;
import io.mycat.net.handler.FrontendQueryHandler;
import io.mycat.net.mysql.OkPacket;
import io.mycat.server.handler.*;
import io.mycat.server.parser.ServerParse;

/**
 * @author mycat
 */
public class ServerQueryHandler implements FrontendQueryHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ServerQueryHandler.class);

	private final ServerConnection source;
	protected Boolean readOnly;

	public void setReadOnly(Boolean readOnly) {
		this.readOnly = readOnly;
	}

	public ServerQueryHandler(ServerConnection source) {
		this.source = source;
	}

	@Override
	public void query(String sql) {
		
		ServerConnection c = this.source;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(new StringBuilder().append(c).append(sql).toString());
		}
		//
		int rs = ServerParse.parse(sql);
		int sqlType = rs & 0xff;
		
		switch (sqlType) {
		//explain sql
		case ServerParse.EXPLAIN:
			ExplainHandler.handle(sql, c, rs >>> 8);
			break;
		//explain2 datanode=? sql=?
		case ServerParse.EXPLAIN2:
			Explain2Handler.handle(sql, c, rs >>> 8);
			break;
		case ServerParse.SET:
			SetHandler.handle(sql, c, rs >>> 8);
			break;
		case ServerParse.SHOW:
			ShowHandler.handle(sql, c, rs >>> 8);
			break;
		case ServerParse.SELECT:
			SelectHandler.handle(sql, c, rs >>> 8);
			break;
		case ServerParse.START:
			StartHandler.handle(sql, c, rs >>> 8);
			break;
		case ServerParse.BEGIN:
			BeginHandler.handle(sql, c);
			break;
		//不支持oracle的savepoint事务回退点
		case ServerParse.SAVEPOINT:
			SavepointHandler.handle(sql, c);
			break;
		case ServerParse.KILL:
			KillHandler.handle(sql, rs >>> 8, c);
			break;
		//不支持KILL_Query
		case ServerParse.KILL_QUERY:
			LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
			c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,"Unsupported command");
			break;
		case ServerParse.USE:
			UseHandler.handle(sql, c, rs >>> 8);
			break;
		case ServerParse.COMMIT:
			c.commit();
			break;
		case ServerParse.ROLLBACK:
			c.rollback();
			break;
		case ServerParse.HELP:
			LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
			c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
			break;
		case ServerParse.MYSQL_CMD_COMMENT:
			c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
			break;
		case ServerParse.MYSQL_COMMENT:
			c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
			break;
        case ServerParse.LOAD_DATA_INFILE_SQL:
            c.loadDataInfileStart(sql);
            break;
		case ServerParse.MIGRATE:
			MigrateHandler.handle(sql,c);
			break;
		case ServerParse.LOCK:
        	c.lockTable(sql);
        	break;
        case ServerParse.UNLOCK:
        	c.unLockTable(sql);
        	break;
		default:
			if(readOnly){
				LOGGER.warn(new StringBuilder().append("User readonly:").append(sql).toString());
				c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User readonly");
				break;
			}
			c.execute(sql, rs & 0xff);
		}
	}

}
