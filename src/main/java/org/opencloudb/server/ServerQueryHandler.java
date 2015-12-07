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
package org.opencloudb.server;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.handler.FrontendQueryHandler;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.server.handler.*;
import org.opencloudb.server.parser.ServerParse;

/**
 * @author mycat
 */
public class ServerQueryHandler implements FrontendQueryHandler {
	private static final Logger LOGGER = Logger
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
		case ServerParse.EXPLAIN:
			ExplainHandler.handle(sql, c, rs >>> 8);
			break;
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
			if(QuarantineHandler.handle(sql, c)){
				SelectHandler.handle(sql, c, rs >>> 8);
			}
			break;
		case ServerParse.START:
			StartHandler.handle(sql, c, rs >>> 8);
			break;
		case ServerParse.BEGIN:
			BeginHandler.handle(sql, c);
			break;
		case ServerParse.SAVEPOINT:
			SavepointHandler.handle(sql, c);
			break;
		case ServerParse.KILL:
			KillHandler.handle(sql, rs >>> 8, c);
			break;
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
		default:
			if(readOnly){
				LOGGER.warn(new StringBuilder().append("User readonly:").append(sql).toString());
				c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User readonly");
				break;
			}
			if(QuarantineHandler.handle(sql, c)){
				c.execute(sql, rs & 0xff);
			}
		}
	}

}