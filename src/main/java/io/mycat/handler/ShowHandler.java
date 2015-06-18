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
package io.mycat.handler;

import io.mycat.config.ErrorCode;
import io.mycat.manager.ManagerConnection;
import io.mycat.parser.ManagerParseShow;
import io.mycat.parser.util.ParseUtil;
import io.mycat.response.ShowBackend;
import io.mycat.response.ShowCollation;
import io.mycat.response.ShowCommand;
import io.mycat.response.ShowConnection;
import io.mycat.response.ShowConnectionSQL;
import io.mycat.response.ShowDataNode;
import io.mycat.response.ShowDataSource;
import io.mycat.response.ShowDatabase;
import io.mycat.response.ShowHeartbeat;
import io.mycat.response.ShowHelp;
import io.mycat.response.ShowParser;
import io.mycat.response.ShowProcessor;
import io.mycat.response.ShowRouter;
import io.mycat.response.ShowSQL;
import io.mycat.response.ShowSQLDetail;
import io.mycat.response.ShowSQLExecute;
import io.mycat.response.ShowSQLSlow;
import io.mycat.response.ShowServer;
import io.mycat.response.ShowSession;
import io.mycat.response.ShowThreadPool;
import io.mycat.response.ShowTime;
import io.mycat.response.ShowVariables;
import io.mycat.response.ShowVersion;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public final class ShowHandler {

	public static void handle(String stmt, ManagerConnection c, int offset) {
		int rs = ManagerParseShow.parse(stmt, offset);
		switch (rs & 0xff) {
		case ManagerParseShow.COMMAND:
			ShowCommand.execute(c);
			break;
		case ManagerParseShow.COLLATION:
			ShowCollation.execute(c);
			break;
		case ManagerParseShow.CONNECTION:
			ShowConnection.execute(c);
			break;
		case ManagerParseShow.BACKEND:
			ShowBackend.execute(c);
			break;
		case ManagerParseShow.CONNECTION_SQL:
			ShowConnectionSQL.execute(c);
			break;
		case ManagerParseShow.DATABASE:
			ShowDatabase.execute(c);
			break;
		case ManagerParseShow.DATANODE:
			ShowDataNode.execute(c, null);
			break;
		case ManagerParseShow.DATANODE_WHERE: {
			String name = stmt.substring(rs >>> 8).trim();
			if (StringUtil.isEmpty(name)) {
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
			} else {
				ShowDataNode.execute(c, name);
			}
			break;
		}
		case ManagerParseShow.DATASOURCE:
			ShowDataSource.execute(c, null);
			break;
		case ManagerParseShow.DATASOURCE_WHERE: {
			String name = stmt.substring(rs >>> 8).trim();
			if (StringUtil.isEmpty(name)) {
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
			} else {
				ShowDataSource.execute(c, name);
			}
			break;
		}
		case ManagerParseShow.HELP:
			ShowHelp.execute(c);
			break;
		case ManagerParseShow.HEARTBEAT:
			ShowHeartbeat.response(c);
			break;
		case ManagerParseShow.PARSER:
			ShowParser.execute(c);
			break;
		case ManagerParseShow.PROCESSOR:
			ShowProcessor.execute(c);
			break;
		case ManagerParseShow.ROUTER:
			ShowRouter.execute(c);
			break;
		case ManagerParseShow.SERVER:
			ShowServer.execute(c);
			break;
		case ManagerParseShow.SQL:
			ShowSQL.execute(c, ParseUtil.getSQLId(stmt));
			break;
		case ManagerParseShow.SQL_DETAIL:
			ShowSQLDetail.execute(c, ParseUtil.getSQLId(stmt));
			break;
		case ManagerParseShow.SQL_EXECUTE:
			ShowSQLExecute.execute(c);
			break;
		case ManagerParseShow.SQL_SLOW:
			ShowSQLSlow.execute(c);
			break;
		case ManagerParseShow.SLOW_DATANODE: {
			String name = stmt.substring(rs >>> 8).trim();
			if (StringUtil.isEmpty(name)) {
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
			} else {
				// ShowSlow.dataNode(c, name);
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
			}
			break;
		}
		case ManagerParseShow.SLOW_SCHEMA: {
			String name = stmt.substring(rs >>> 8).trim();
			if (StringUtil.isEmpty(name)) {
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
			} else {
				c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
				// ShowSlow.schema(c, name);
			}
			break;
		}
		case ManagerParseShow.THREADPOOL:
			ShowThreadPool.execute(c);
			break;
		case ManagerParseShow.CACHE:
			ShowCache.execute(c);
			break;
		case ManagerParseShow.SESSION:
			ShowSession.execute(c);
			break;
		case ManagerParseShow.TIME_CURRENT:
			ShowTime.execute(c, ManagerParseShow.TIME_CURRENT);
			break;
		case ManagerParseShow.TIME_STARTUP:
			ShowTime.execute(c, ManagerParseShow.TIME_STARTUP);
			break;
		case ManagerParseShow.VARIABLES:
			ShowVariables.execute(c);
			break;
		case ManagerParseShow.VERSION:
			ShowVersion.execute(c);
			break;
		default:
			c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
		}
	}
}