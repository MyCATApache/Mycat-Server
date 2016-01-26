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
package org.opencloudb.handler;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.parser.ManagerParseShow;
import org.opencloudb.parser.util.ParseUtil;
import org.opencloudb.response.ShowBackend;
import org.opencloudb.response.ShowCollation;
import org.opencloudb.response.ShowCommand;
import org.opencloudb.response.ShowConnection;
import org.opencloudb.response.ShowConnectionSQL;
import org.opencloudb.response.ShowDataNode;
import org.opencloudb.response.ShowDataSource;
import org.opencloudb.response.ShowDatabase;
import org.opencloudb.response.ShowDatasourceCluster;
import org.opencloudb.response.ShowDatasourceSyn;
import org.opencloudb.response.ShowDatasourceSynDetail;
import org.opencloudb.response.ShowHeartbeat;
import org.opencloudb.response.ShowHeartbeatDetail;
import org.opencloudb.response.ShowHelp;
import org.opencloudb.response.ShowParser;
import org.opencloudb.response.ShowProcessor;
import org.opencloudb.response.ShowRouter;
import org.opencloudb.response.ShowSQL;
import org.opencloudb.response.ShowSQLCondition;
import org.opencloudb.response.ShowSQLDetail;
import org.opencloudb.response.ShowSQLExecute;
import org.opencloudb.response.ShowSQLHigh;
import org.opencloudb.response.ShowSQLSlow;
import org.opencloudb.response.ShowSQLSumTable;
import org.opencloudb.response.ShowSQLSumUser;
import org.opencloudb.response.ShowServer;
import org.opencloudb.response.ShowSession;
import org.opencloudb.response.ShowSysLog;
import org.opencloudb.response.ShowSysParam;
import org.opencloudb.response.ShowThreadPool;
import org.opencloudb.response.ShowTime;
import org.opencloudb.response.ShowVariables;
import org.opencloudb.response.ShowVersion;
import org.opencloudb.response.ShowWhiteHost;
import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
public final class ShowHandler {

	public static void handle(String stmt, ManagerConnection c, int offset) {
		int rs = ManagerParseShow.parse(stmt, offset);
		switch (rs & 0xff) {
		case ManagerParseShow.SYSPARAM://add rainbow
			ShowSysParam.execute(c);
			break;
		case ManagerParseShow.SYSLOG: //add by zhuam
			String lines = stmt.substring(rs >>> 8).trim();
			ShowSysLog.execute(c, Integer.parseInt( lines ) );
			break;
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
		case ManagerParseShow.WHITE_HOST:
			ShowWhiteHost.execute(c);
			break;
		case ManagerParseShow.WHITE_HOST_SET:
			ShowWhiteHost.setHost(c,ParseUtil.parseString(stmt));
			break;					
		case ManagerParseShow.SQL:
			boolean isClearSql = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
			ShowSQL.execute(c, isClearSql);
			break;
		case ManagerParseShow.SQL_DETAIL:
			ShowSQLDetail.execute(c, ParseUtil.getSQLId(stmt));
			break;
		case ManagerParseShow.SQL_EXECUTE:
			ShowSQLExecute.execute(c);
			break;
		case ManagerParseShow.SQL_SLOW:
			boolean isClearSlow = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
			ShowSQLSlow.execute(c, isClearSlow);
			break;
		case ManagerParseShow.SQL_HIGH:
			boolean isClearHigh = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
			ShowSQLHigh.execute(c, isClearHigh);
			break;
		case ManagerParseShow.SQL_CONDITION:
			ShowSQLCondition.execute(c);
			break;			
		case ManagerParseShow.SQL_SUM_USER:
			boolean isClearSum = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
			ShowSQLSumUser.execute(c,isClearSum);
			break;
		case ManagerParseShow.SQL_SUM_TABLE:
			boolean isClearTable = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
			ShowSQLSumTable.execute(c, isClearTable);
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
		case ManagerParseShow.HEARTBEAT_DETAIL://by songwie
			ShowHeartbeatDetail.response(c,stmt);
			break;
		case ManagerParseShow.DATASOURCE_SYNC://by songwie
			ShowDatasourceSyn.response(c,stmt);
			break;	
		case ManagerParseShow.DATASOURCE_SYNC_DETAIL://by songwie
			ShowDatasourceSynDetail.response(c,stmt);
			break;	
		case ManagerParseShow.DATASOURCE_CLUSTER://by songwie
			ShowDatasourceCluster.response(c,stmt);
			break;	
		default:
			c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
		}
	}
}