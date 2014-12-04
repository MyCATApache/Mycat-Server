/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights
 * reserved. DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER. This
 * code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation. This code is distributed in the hope that it will
 * be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License version 2 for more details (a copy is included in the LICENSE
 * file that accompanied this code). You should have received a copy of the GNU
 * General Public License version 2 along with this work; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA. Any questions about this component can be directed to it's
 * project Web address https://code.google.com/p/opencloudb/.
 */
package org.opencloudb.parser;

import java.sql.SQLSyntaxErrorException;

import org.opencloudb.MycatServer;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserFeature;

/**
 * @author mycat
 */
public final class SQLParserDelegate {
	/**
     * 
     */

	public static final String DEFAULT_CHARSET = "utf-8";

	// private static final ThreadLocal<SQLParser> sqlParser = new
	// ThreadLocal<SQLParser>() {
	// protected SQLParser initialValue() {
	// SQLParser parser = new SQLParser();
	// parser.getFeatures().add(SQLParserFeature.DOUBLE_QUOTED_STRING);
	// parser.getFeatures().add(SQLParserFeature.MYSQL_HINTS);
	// parser.getFeatures().add(SQLParserFeature.MYSQL_INTERVAL);
	// // fix 位操作符号解析问题 add by micmiu
	// parser.getFeatures().add(SQLParserFeature.INFIX_BIT_OPERATORS);
	// // fix 最大解析文本限制 add by 石头狮子
	// parser.setMaxStringLiteralLength(MycatServer.getInstance()
	// .getConfig().getSystem().getMaxStringLiteralLength());
	// return parser;
	// }
	//
	// };

	public static QueryTreeNode parse(String stmt, String string)
			throws SQLSyntaxErrorException {
		try {
			// return sqlParser.get().parseStatement(stmt);
			SQLParser parser = new SQLParser();
			parser.getFeatures().add(SQLParserFeature.DOUBLE_QUOTED_STRING);
			parser.getFeatures().add(SQLParserFeature.MYSQL_HINTS);
			parser.getFeatures().add(SQLParserFeature.MYSQL_INTERVAL);
			// fix 位操作符号解析问题 add by micmiu
			parser.getFeatures().add(SQLParserFeature.INFIX_BIT_OPERATORS);
			// fix 最大解析文本限制 add by 石头狮子
			parser.setMaxStringLiteralLength(MycatServer.getInstance()
					.getConfig().getSystem().getMaxStringLiteralLength());
			return parser.parseStatement(stmt);
		} catch (StandardException e) {
			throw new SQLSyntaxErrorException(e);
		}
	}

}