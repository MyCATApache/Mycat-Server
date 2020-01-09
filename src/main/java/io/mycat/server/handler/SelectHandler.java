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

import io.mycat.route.parser.util.ParseUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.parser.ServerParseSelect;
import io.mycat.server.response.*;

/**
 * @author mycat
 */
public final class SelectHandler {

	public static void handle(String stmt, ServerConnection c, int offs) {
		int offset = offs;
		switch (ServerParseSelect.parse(stmt, offs)) {
		case ServerParseSelect.VERSION_COMMENT:
			SelectVersionComment.response(c);
			break;
		case ServerParseSelect.DATABASE:
			SelectDatabase.response(c);
			break;
		case ServerParseSelect.USER:
			SelectUser.response(c);
			break;
		case ServerParseSelect.VERSION:
			SelectVersion.response(c);
			break;
		case ServerParseSelect.SESSION_INCREMENT:
			SessionIncrement.response(c);
			break;
		case ServerParseSelect.SESSION_ISOLATION:
			SessionIsolation.response(c);
			break;
		case ServerParseSelect.LAST_INSERT_ID:
			// offset = ParseUtil.move(stmt, 0, "select".length());
			loop:for (int l=stmt.length(); offset < l; ++offset) {
				switch (stmt.charAt(offset)) {
				case ' ':
					continue;
				case '/':
				case '#':
					offset = ParseUtil.comment(stmt, offset);
					continue;
				case 'L':
				case 'l':
					break loop;
				}
			}
			offset = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, offset);
			offset = ServerParseSelect.skipAs(stmt, offset);
			SelectLastInsertId.response(c, stmt, offset);
			break;
		case ServerParseSelect.IDENTITY:
			// offset = ParseUtil.move(stmt, 0, "select".length());
			loop:for (int l=stmt.length(); offset < l; ++offset) {
				switch (stmt.charAt(offset)) {
				case ' ':
					continue;
				case '/':
				case '#':
					offset = ParseUtil.comment(stmt, offset);
					continue;
				case '@':
					break loop;
				}
			}
			int indexOfAtAt = offset;
			offset += 2;
			offset = ServerParseSelect.indexAfterIdentity(stmt, offset);
			String orgName = stmt.substring(indexOfAtAt, offset);
			offset = ServerParseSelect.skipAs(stmt, offset);
			SelectIdentity.response(c, stmt, offset, orgName);
			break;
            case ServerParseSelect.SELECT_VAR_ALL:
				c.execute(stmt, ServerParse.SELECT);
				break;
			case ServerParseSelect.SESSION_TX_READ_ONLY:
				SelectTxReadOnly.response(c);
				break;
		default:
			c.execute(stmt, ServerParse.SELECT);
		}
	}

}
