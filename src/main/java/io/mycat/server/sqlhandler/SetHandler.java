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
package io.mycat.server.sqlhandler;

import io.mycat.server.ErrorCode;
import io.mycat.server.Isolations;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.packet.OkPacket;
import io.mycat.server.parser.ServerParseSet;
import io.mycat.server.response.CharacterSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.server.parser.ServerParseSet.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SET 语句处理
 * 
 * @author mycat
 */
public final class SetHandler {

	private static final Logger logger = LoggerFactory.getLogger(SetHandler.class);
	
	private static final byte[] AC_OFF = new byte[] { 7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0 };
	
	private static List<Pattern> ptrnIgnoreList = new ArrayList<Pattern>();
	
	static  {
		
		//TODO: 忽略部分 SET 指令, 避免WARN 不断的刷日志
		String[] ignores = new String[] {
			"(?i)set (sql_mode)",
			"(?i)set (interactive_timeout|wait_timeout|net_read_timeout|net_write_timeout|lock_wait_timeout|slave_net_timeout)",
			"(?i)set (connect_timeout|delayed_insert_timeout|innodb_lock_wait_timeout|innodb_rollback_on_timeout)",
			"(?i)set (profiling|profiling_history_size)"
		};
		
		for (int i = 0; i < ignores.length; ++i) {
            ptrnIgnoreList.add(Pattern.compile(ignores[i]));
        }
	}	

	public static void handle(String stmt, MySQLFrontConnection c, int offset) {
		// System.out.println("SetHandler: "+stmt);
		int rs = ServerParseSet.parse(stmt, offset);
		switch (rs & 0xff) {
		case AUTOCOMMIT_ON:
			if (c.isAutocommit()) {
				c.write(OkPacket.OK);
			} else {
				c.commit();
				c.setAutocommit(true);
			}
			break;
		case AUTOCOMMIT_OFF: {
			if (c.isAutocommit()) {
				c.setAutocommit(false);
			}
			c.write(AC_OFF);
			break;
		}
		case XA_FLAG_ON: {
			if (c.isAutocommit()) {
				c.writeErrMessage(ErrorCode.ERR_WRONG_USED,
						"set xa cmd on can't used in autocommit connection ");
				return;
			}
			c.getSession2().setXATXEnabled(true);
			c.write(OkPacket.OK);
			break;
		}
		case XA_FLAG_OFF: {
			c.writeErrMessage(ErrorCode.ERR_WRONG_USED,
					"set xa cmd off not for external use ");
			return;
		}
		case TX_READ_UNCOMMITTED: {
			c.setTxIsolation(Isolations.READ_UNCOMMITTED);
			c.write(OkPacket.OK);
			break;
		}
		case TX_READ_COMMITTED: {
			c.setTxIsolation(Isolations.READ_COMMITTED);
			c.write(OkPacket.OK);
			break;
		}
		case TX_REPEATED_READ: {
			c.setTxIsolation(Isolations.REPEATED_READ);
			c.write(OkPacket.OK);
			break;
		}
		case TX_SERIALIZABLE: {
			c.setTxIsolation(Isolations.SERIALIZABLE);
			c.write(OkPacket.OK);
			break;
		}
		case NAMES:
			String charset = stmt.substring(rs >>> 8).trim();
			if (c.setCharset(charset)) {
				c.write(OkPacket.OK);
			} else {
				c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
						"Unknown charset '" + charset + "'");
			}
			break;
		case CHARACTER_SET_CLIENT:
		case CHARACTER_SET_CONNECTION:
		case CHARACTER_SET_RESULTS:
			CharacterSet.response(stmt, c, rs);
			break;
		default:
			boolean ignore = false;
	         Matcher matcherIgnore;
	         for (Pattern ptrnIgnore : ptrnIgnoreList) {
	             matcherIgnore = ptrnIgnore.matcher( stmt );
	             if (matcherIgnore.find()) {
	                 ignore = true;
	                 break;
	             }
	         }
			
            if ( !ignore ) {        	 
    			StringBuilder s = new StringBuilder();
    			logger.warn(s.append(c).append(stmt).append(" is not recoginized and ignored").toString());
            }
            c.write(OkPacket.OK);
		}
	}

}