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

import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.parser.ServerParseShow;
import io.mycat.server.response.*;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public final class ShowHandler {

	public static void handle(String stmt, ServerConnection c, int offset) {

		// 排除 “ ` ” 符号
		stmt = StringUtil.replaceChars(stmt, "`", null);

		int type = ServerParseShow.parse(stmt, offset);
		switch (type) {
		case ServerParseShow.DATABASES:
			ShowDatabases.response(c);
			break;
		case ServerParseShow.TABLES:
			ShowTables.response(c, stmt,type);
			break;
            case ServerParseShow.FULLTABLES:
                ShowFullTables.response(c, stmt,type);
                break;
		case ServerParseShow.MYCAT_STATUS:
			ShowMyCatStatus.response(c);
			break;
		case ServerParseShow.MYCAT_CLUSTER:
			ShowMyCATCluster.response(c);
			break;
		default:
			c.execute(stmt, ServerParse.SHOW);
		}
	}

}