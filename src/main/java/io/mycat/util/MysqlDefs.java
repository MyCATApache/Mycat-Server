/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU AFFERO GENERAL PUBLIC LICENSE as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU AFFERO GENERAL PUBLIC LICENSE for more details.

 You should have received a copy of the GNU AFFERO GENERAL PUBLIC LICENSE
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
/*
 * 	This program is free software; you can redistribute it and/or modify it under the terms of 
 * the GNU AFFERO GENERAL PUBLIC LICENSE as published by the Free Software Foundation; either version 3 of the License, 
 * or (at your option) any later version. 
 * 
 * 	This program is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
 * See the GNU AFFERO GENERAL PUBLIC LICENSE for more details. 
 * 	You should have received a copy of the GNU AFFERO GENERAL PUBLIC LICENSE along with this program; 
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package io.mycat.util;

import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * copy from mysql-connector-j MysqlDefs contains many values that are needed
 * for communication with the MySQL server.
 * 
 * @author Mark Matthews
 * @version $Id: MysqlDefs.java 4724 2005-12-20 23:27:01Z mmatthews $
 */
public final class MysqlDefs {
	// ~ Static fields/initializers
	// ---------------------------------------------

	public static final int COM_BINLOG_DUMP = 18;

	public static final int COM_CHANGE_USER = 17;

	public static final int COM_CLOSE_STATEMENT = 25;

	public static final int COM_CONNECT_OUT = 20;

	public static final int COM_END = 29;

	public static final int COM_EXECUTE = 23;

	public static final int COM_FETCH = 28;

	public static final int COM_LONG_DATA = 24;

	public static final int COM_PREPARE = 22;

	public static final int COM_REGISTER_SLAVE = 21;

	public static final int COM_RESET_STMT = 26;

	public static final int COM_SET_OPTION = 27;

	public static final int COM_TABLE_DUMP = 19;

	public static final int CONNECT = 11;

	public static final int CREATE_DB = 5;

	public static final int DEBUG = 13;

	public static final int DELAYED_INSERT = 16;

	public static final int DROP_DB = 6;

	public static final int FIELD_LIST = 4;

	public static final int FIELD_TYPE_BIT = 16;

	public static final int FIELD_TYPE_BLOB = 252;

	public static final int FIELD_TYPE_DATE = 10;

	public static final int FIELD_TYPE_DATETIME = 12;

	// Data Types
	public static final int FIELD_TYPE_DECIMAL = 0;

	public static final int FIELD_TYPE_DOUBLE = 5;

	public static final int FIELD_TYPE_ENUM = 247;

	public static final int FIELD_TYPE_FLOAT = 4;

	public static final int FIELD_TYPE_GEOMETRY = 255;

	public static final int FIELD_TYPE_INT24 = 9;

	public static final int FIELD_TYPE_LONG = 3;

	public static final int FIELD_TYPE_LONG_BLOB = 251;

	public static final int FIELD_TYPE_LONGLONG = 8;

	public static final int FIELD_TYPE_MEDIUM_BLOB = 250;

	public static final int FIELD_TYPE_NEW_DECIMAL = 246;

	public static final int FIELD_TYPE_NEWDATE = 14;

	public static final int FIELD_TYPE_NULL = 6;

	public static final int FIELD_TYPE_SET = 248;

	public static final int FIELD_TYPE_SHORT = 2;

	public static final int FIELD_TYPE_STRING = 254;

	public static final int FIELD_TYPE_TIME = 11;

	public static final int FIELD_TYPE_TIMESTAMP = 7;

	public static final int FIELD_TYPE_TINY = 1;

	// Older data types
	public static final int FIELD_TYPE_TINY_BLOB = 249;

	public static final int FIELD_TYPE_VAR_STRING = 253;

	public static final int FIELD_TYPE_VARCHAR = 15;

	// Newer data types
	public static final int FIELD_TYPE_YEAR = 13;

	public static final int INIT_DB = 2;

	public static final long LENGTH_BLOB = 65535;

	public static final long LENGTH_LONGBLOB = 4294967295L;

	public static final long LENGTH_MEDIUMBLOB = 16777215;

	public static final long LENGTH_TINYBLOB = 255;

	// Limitations
	public static final int MAX_ROWS = 50000000; // From the MySQL FAQ

	/**
	 * Used to indicate that the server sent no field-level character set
	 * information, so the driver should use the connection-level character
	 * encoding instead.
	 */
	public static final int NO_CHARSET_INFO = -1;

	public static final byte OPEN_CURSOR_FLAG = 1;

	public static final int PING = 14;

	public static final int PROCESS_INFO = 10;

	public static final int PROCESS_KILL = 12;

	public static final int QUERY = 3;

	public static final int QUIT = 1;

	// ~ Methods
	// ----------------------------------------------------------------

	public static final int RELOAD = 7;

	public static final int SHUTDOWN = 8;

	//
	// Constants defined from mysql
	//
	// DB Operations
	public static final int SLEEP = 0;

	public static final int STATISTICS = 9;

	public static final int TIME = 15;

	/**
	 * Maps the given MySQL type to the correct JDBC type.
	 */
	public static int mysqlToJavaType(int mysqlType) {
		int jdbcType;

		switch (mysqlType) {
		case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
		case MysqlDefs.FIELD_TYPE_DECIMAL:
			jdbcType = Types.DECIMAL;

			break;

		case MysqlDefs.FIELD_TYPE_TINY:
			jdbcType = Types.TINYINT;

			break;

		case MysqlDefs.FIELD_TYPE_SHORT:
			jdbcType = Types.SMALLINT;

			break;

		case MysqlDefs.FIELD_TYPE_LONG:
			jdbcType = Types.INTEGER;

			break;

		case MysqlDefs.FIELD_TYPE_FLOAT:
			jdbcType = Types.REAL;

			break;

		case MysqlDefs.FIELD_TYPE_DOUBLE:
			jdbcType = Types.DOUBLE;

			break;

		case MysqlDefs.FIELD_TYPE_NULL:
			jdbcType = Types.NULL;

			break;

		case MysqlDefs.FIELD_TYPE_TIMESTAMP:
			jdbcType = Types.TIMESTAMP;

			break;

		case MysqlDefs.FIELD_TYPE_LONGLONG:
			jdbcType = Types.BIGINT;

			break;

		case MysqlDefs.FIELD_TYPE_INT24:
			jdbcType = Types.INTEGER;

			break;

		case MysqlDefs.FIELD_TYPE_DATE:
			jdbcType = Types.DATE;

			break;

		case MysqlDefs.FIELD_TYPE_TIME:
			jdbcType = Types.TIME;

			break;

		case MysqlDefs.FIELD_TYPE_DATETIME:
			jdbcType = Types.TIMESTAMP;

			break;

		case MysqlDefs.FIELD_TYPE_YEAR:
			jdbcType = Types.DATE;

			break;

		case MysqlDefs.FIELD_TYPE_NEWDATE:
			jdbcType = Types.DATE;

			break;

		case MysqlDefs.FIELD_TYPE_ENUM:
			jdbcType = Types.CHAR;

			break;

		case MysqlDefs.FIELD_TYPE_SET:
			jdbcType = Types.CHAR;

			break;

		case MysqlDefs.FIELD_TYPE_TINY_BLOB:
			jdbcType = Types.VARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
			jdbcType = Types.LONGVARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_LONG_BLOB:
			jdbcType = Types.LONGVARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_BLOB:
			jdbcType = Types.LONGVARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_VAR_STRING:
		case MysqlDefs.FIELD_TYPE_VARCHAR:
			jdbcType = Types.VARCHAR;

			break;

		case MysqlDefs.FIELD_TYPE_STRING:
			jdbcType = Types.CHAR;

			break;
		case MysqlDefs.FIELD_TYPE_GEOMETRY:
			jdbcType = Types.BINARY;

			break;
		case MysqlDefs.FIELD_TYPE_BIT:
			jdbcType = Types.BIT;

			break;
		default:
			jdbcType = Types.VARCHAR;
		}

		return jdbcType;
	}

	public static int javaTypeDetect(int javaType, int scale) {
		switch (javaType) {
		case Types.NUMERIC: {
			if (scale > 0) {
				return Types.DECIMAL;
			}else{
				return javaType;
			}
		}
		default: {
			return javaType;
		}
		}

	}

	public static int javaTypeMysql(int javaType) {

		switch (javaType) {
		case Types.NUMERIC:
			return MysqlDefs.FIELD_TYPE_DECIMAL;

		case Types.DECIMAL:
			return MysqlDefs.FIELD_TYPE_NEW_DECIMAL;

		case Types.TINYINT:
			return MysqlDefs.FIELD_TYPE_TINY;

		case Types.SMALLINT:
			return MysqlDefs.FIELD_TYPE_SHORT;

		case Types.INTEGER:
			return MysqlDefs.FIELD_TYPE_LONG;

		case Types.REAL:
			return MysqlDefs.FIELD_TYPE_FLOAT;

		case Types.DOUBLE:
			return MysqlDefs.FIELD_TYPE_DOUBLE;

		case Types.NULL:
			return MysqlDefs.FIELD_TYPE_NULL;

		case Types.TIMESTAMP:
			return MysqlDefs.FIELD_TYPE_TIMESTAMP;

		case Types.BIGINT:
			return MysqlDefs.FIELD_TYPE_LONGLONG;

		case Types.DATE:
			return MysqlDefs.FIELD_TYPE_DATE;

		case Types.TIME:
			return MysqlDefs.FIELD_TYPE_TIME;

		case Types.VARBINARY:
			return MysqlDefs.FIELD_TYPE_TINY_BLOB;

		case Types.LONGVARBINARY:
			return MysqlDefs.FIELD_TYPE_BLOB;
            //对应sqlserver的image类型
            case 27:
                return MysqlDefs.FIELD_TYPE_BLOB;

		case Types.VARCHAR:
			return MysqlDefs.FIELD_TYPE_VAR_STRING;

		case Types.CHAR:
			return MysqlDefs.FIELD_TYPE_STRING;

		case Types.BINARY:
			return MysqlDefs.FIELD_TYPE_GEOMETRY;

		case Types.BIT:
			return MysqlDefs.FIELD_TYPE_BIT;
		case Types.CLOB:
			return MysqlDefs.FIELD_TYPE_VAR_STRING;
		case Types.BLOB:
			return MysqlDefs.FIELD_TYPE_BLOB;

		//修改by     magicdoom@gmail.com
		// 当jdbc连接非mysql的数据库时，需要把对应类型映射为mysql的类型，否则应用端会出错
			case Types.NVARCHAR:
				return MysqlDefs.FIELD_TYPE_VAR_STRING;
			case Types.NCHAR:
				return MysqlDefs.FIELD_TYPE_STRING;
			case Types.NCLOB:
				return MysqlDefs.FIELD_TYPE_VAR_STRING;
			case Types.LONGNVARCHAR:
				return MysqlDefs.FIELD_TYPE_VAR_STRING;

		default:
			return MysqlDefs.FIELD_TYPE_VAR_STRING;   //其他未知类型返回字符类型
		//	return Types.VARCHAR;
		}

	}

	/**
	 * Maps the given MySQL type to the correct JDBC type.
	 */
	static int mysqlToJavaType(String mysqlType) {
		if (mysqlType.equalsIgnoreCase("BIT")) {
			return mysqlToJavaType(FIELD_TYPE_BIT);
		} else if (mysqlType.equalsIgnoreCase("TINYINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_TINY);
		} else if (mysqlType.equalsIgnoreCase("SMALLINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_SHORT);
		} else if (mysqlType.equalsIgnoreCase("MEDIUMINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_INT24);
		} else if (mysqlType.equalsIgnoreCase("INT") || mysqlType.equalsIgnoreCase("INTEGER")) { //$NON-NLS-1$ //$NON-NLS-2$
			return mysqlToJavaType(FIELD_TYPE_LONG);
		} else if (mysqlType.equalsIgnoreCase("BIGINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_LONGLONG);
		} else if (mysqlType.equalsIgnoreCase("INT24")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_INT24);
		} else if (mysqlType.equalsIgnoreCase("REAL")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DOUBLE);
		} else if (mysqlType.equalsIgnoreCase("FLOAT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_FLOAT);
		} else if (mysqlType.equalsIgnoreCase("DECIMAL")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DECIMAL);
		} else if (mysqlType.equalsIgnoreCase("NUMERIC")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DECIMAL);
		} else if (mysqlType.equalsIgnoreCase("DOUBLE")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DOUBLE);
		} else if (mysqlType.equalsIgnoreCase("CHAR")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_STRING);
		} else if (mysqlType.equalsIgnoreCase("VARCHAR")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_VAR_STRING);
		} else if (mysqlType.equalsIgnoreCase("DATE")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DATE);
		} else if (mysqlType.equalsIgnoreCase("TIME")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_TIME);
		} else if (mysqlType.equalsIgnoreCase("YEAR")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_YEAR);
		} else if (mysqlType.equalsIgnoreCase("TIMESTAMP")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_TIMESTAMP);
		} else if (mysqlType.equalsIgnoreCase("DATETIME")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DATETIME);
		} else if (mysqlType.equalsIgnoreCase("TINYBLOB")) { //$NON-NLS-1$
			return java.sql.Types.BINARY;
		} else if (mysqlType.equalsIgnoreCase("BLOB")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARBINARY;
		} else if (mysqlType.equalsIgnoreCase("MEDIUMBLOB")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARBINARY;
		} else if (mysqlType.equalsIgnoreCase("LONGBLOB")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARBINARY;
		} else if (mysqlType.equalsIgnoreCase("TINYTEXT")) { //$NON-NLS-1$
			return java.sql.Types.VARCHAR;
		} else if (mysqlType.equalsIgnoreCase("TEXT")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARCHAR;
		} else if (mysqlType.equalsIgnoreCase("MEDIUMTEXT")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARCHAR;
		} else if (mysqlType.equalsIgnoreCase("LONGTEXT")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARCHAR;
		} else if (mysqlType.equalsIgnoreCase("ENUM")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_ENUM);
		} else if (mysqlType.equalsIgnoreCase("SET")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_SET);
		} else if (mysqlType.equalsIgnoreCase("GEOMETRY")) {
			return mysqlToJavaType(FIELD_TYPE_GEOMETRY);
		} else if (mysqlType.equalsIgnoreCase("BINARY")) {
			return Types.BINARY; // no concrete type on the wire
		} else if (mysqlType.equalsIgnoreCase("VARBINARY")) {
			return Types.VARBINARY; // no concrete type on the wire
		} 

		// Punt
		return java.sql.Types.OTHER;
	}

	/**
	 * @param mysqlType
	 * @return
	 */
	public static String typeToName(int mysqlType) {
		switch (mysqlType) {
		case MysqlDefs.FIELD_TYPE_DECIMAL:
			return "FIELD_TYPE_DECIMAL";

		case MysqlDefs.FIELD_TYPE_TINY:
			return "FIELD_TYPE_TINY";

		case MysqlDefs.FIELD_TYPE_SHORT:
			return "FIELD_TYPE_SHORT";

		case MysqlDefs.FIELD_TYPE_LONG:
			return "FIELD_TYPE_LONG";

		case MysqlDefs.FIELD_TYPE_FLOAT:
			return "FIELD_TYPE_FLOAT";

		case MysqlDefs.FIELD_TYPE_DOUBLE:
			return "FIELD_TYPE_DOUBLE";

		case MysqlDefs.FIELD_TYPE_NULL:
			return "FIELD_TYPE_NULL";

		case MysqlDefs.FIELD_TYPE_TIMESTAMP:
			return "FIELD_TYPE_TIMESTAMP";

		case MysqlDefs.FIELD_TYPE_LONGLONG:
			return "FIELD_TYPE_LONGLONG";

		case MysqlDefs.FIELD_TYPE_INT24:
			return "FIELD_TYPE_INT24";

		case MysqlDefs.FIELD_TYPE_DATE:
			return "FIELD_TYPE_DATE";

		case MysqlDefs.FIELD_TYPE_TIME:
			return "FIELD_TYPE_TIME";

		case MysqlDefs.FIELD_TYPE_DATETIME:
			return "FIELD_TYPE_DATETIME";

		case MysqlDefs.FIELD_TYPE_YEAR:
			return "FIELD_TYPE_YEAR";

		case MysqlDefs.FIELD_TYPE_NEWDATE:
			return "FIELD_TYPE_NEWDATE";

		case MysqlDefs.FIELD_TYPE_ENUM:
			return "FIELD_TYPE_ENUM";

		case MysqlDefs.FIELD_TYPE_SET:
			return "FIELD_TYPE_SET";

		case MysqlDefs.FIELD_TYPE_TINY_BLOB:
			return "FIELD_TYPE_TINY_BLOB";

		case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
			return "FIELD_TYPE_MEDIUM_BLOB";

		case MysqlDefs.FIELD_TYPE_LONG_BLOB:
			return "FIELD_TYPE_LONG_BLOB";

		case MysqlDefs.FIELD_TYPE_BLOB:
			return "FIELD_TYPE_BLOB";

		case MysqlDefs.FIELD_TYPE_VAR_STRING:
			return "FIELD_TYPE_VAR_STRING";

		case MysqlDefs.FIELD_TYPE_STRING:
			return "FIELD_TYPE_STRING";

		case MysqlDefs.FIELD_TYPE_VARCHAR:
			return "FIELD_TYPE_VARCHAR";

		case MysqlDefs.FIELD_TYPE_GEOMETRY:
			return "FIELD_TYPE_GEOMETRY";

		default:
			return " Unknown MySQL Type # " + mysqlType;
		}
	}

	public static boolean isBianry(byte mysqlType) {
		int type = mysqlType;
		if(type < 0) {
			type += 256;
		}

		if(type == MysqlDefs.FIELD_TYPE_BLOB || type == MysqlDefs.FIELD_TYPE_TINY_BLOB ||
				type == MysqlDefs.FIELD_TYPE_MEDIUM_BLOB || type == MysqlDefs.FIELD_TYPE_LONG_BLOB) {
			return true;
		}

		return false;
	}

	private static Map<String, Integer> mysqlToJdbcTypesMap = new HashMap<String, Integer>();

	static {
		mysqlToJdbcTypesMap.put("BIT", new Integer(
				mysqlToJavaType(FIELD_TYPE_BIT)));

		mysqlToJdbcTypesMap.put("TINYINT", new Integer(
				mysqlToJavaType(FIELD_TYPE_TINY)));
		mysqlToJdbcTypesMap.put("SMALLINT", new Integer(
				mysqlToJavaType(FIELD_TYPE_SHORT)));
		mysqlToJdbcTypesMap.put("MEDIUMINT", new Integer(
				mysqlToJavaType(FIELD_TYPE_INT24)));
		mysqlToJdbcTypesMap.put("INT", new Integer(
				mysqlToJavaType(FIELD_TYPE_LONG)));
		mysqlToJdbcTypesMap.put("INTEGER", new Integer(
				mysqlToJavaType(FIELD_TYPE_LONG)));
		mysqlToJdbcTypesMap.put("BIGINT", new Integer(
				mysqlToJavaType(FIELD_TYPE_LONGLONG)));
		mysqlToJdbcTypesMap.put("INT24", new Integer(
				mysqlToJavaType(FIELD_TYPE_INT24)));
		mysqlToJdbcTypesMap.put("REAL", new Integer(
				mysqlToJavaType(FIELD_TYPE_DOUBLE)));
		mysqlToJdbcTypesMap.put("FLOAT", new Integer(
				mysqlToJavaType(FIELD_TYPE_FLOAT)));
		mysqlToJdbcTypesMap.put("DECIMAL", new Integer(
				mysqlToJavaType(FIELD_TYPE_DECIMAL)));
		mysqlToJdbcTypesMap.put("NUMERIC", new Integer(
				mysqlToJavaType(FIELD_TYPE_DECIMAL)));
		mysqlToJdbcTypesMap.put("DOUBLE", new Integer(
				mysqlToJavaType(FIELD_TYPE_DOUBLE)));
		mysqlToJdbcTypesMap.put("CHAR", new Integer(
				mysqlToJavaType(FIELD_TYPE_STRING)));
		mysqlToJdbcTypesMap.put("VARCHAR", new Integer(
				mysqlToJavaType(FIELD_TYPE_VAR_STRING)));
		mysqlToJdbcTypesMap.put("DATE", new Integer(
				mysqlToJavaType(FIELD_TYPE_DATE)));
		mysqlToJdbcTypesMap.put("TIME", new Integer(
				mysqlToJavaType(FIELD_TYPE_TIME)));
		mysqlToJdbcTypesMap.put("YEAR", new Integer(
				mysqlToJavaType(FIELD_TYPE_YEAR)));
		mysqlToJdbcTypesMap.put("TIMESTAMP", new Integer(
				mysqlToJavaType(FIELD_TYPE_TIMESTAMP)));
		mysqlToJdbcTypesMap.put("DATETIME", new Integer(
				mysqlToJavaType(FIELD_TYPE_DATETIME)));
		mysqlToJdbcTypesMap.put("TINYBLOB", new Integer(java.sql.Types.BINARY));
		mysqlToJdbcTypesMap.put("BLOB", new Integer(
				java.sql.Types.LONGVARBINARY));
		mysqlToJdbcTypesMap.put("MEDIUMBLOB", new Integer(
				java.sql.Types.LONGVARBINARY));
		mysqlToJdbcTypesMap.put("LONGBLOB", new Integer(
				java.sql.Types.LONGVARBINARY));
		mysqlToJdbcTypesMap
				.put("TINYTEXT", new Integer(java.sql.Types.VARCHAR));
		mysqlToJdbcTypesMap
				.put("TEXT", new Integer(java.sql.Types.LONGVARCHAR));
		mysqlToJdbcTypesMap.put("MEDIUMTEXT", new Integer(
				java.sql.Types.LONGVARCHAR));
		mysqlToJdbcTypesMap.put("LONGTEXT", new Integer(
				java.sql.Types.LONGVARCHAR));
		mysqlToJdbcTypesMap.put("ENUM", new Integer(
				mysqlToJavaType(FIELD_TYPE_ENUM)));
		mysqlToJdbcTypesMap.put("SET", new Integer(
				mysqlToJavaType(FIELD_TYPE_SET)));
		mysqlToJdbcTypesMap.put("GEOMETRY", new Integer(
				mysqlToJavaType(FIELD_TYPE_GEOMETRY)));
	}

	static final void appendJdbcTypeMappingQuery(StringBuffer buf,
			String mysqlTypeColumnName) {

		buf.append("CASE ");
		Map<String, Integer> typesMap = new HashMap<String, Integer>();
		typesMap.putAll(mysqlToJdbcTypesMap);
		typesMap.put("BINARY", new Integer(Types.BINARY));
		typesMap.put("VARBINARY", new Integer(Types.VARBINARY));

		Iterator<String> mysqlTypes = typesMap.keySet().iterator();

		while (mysqlTypes.hasNext()) {
			String mysqlTypeName = (String) mysqlTypes.next();
			buf.append(" WHEN ");
			buf.append(mysqlTypeColumnName);
			buf.append("='");
			buf.append(mysqlTypeName);
			buf.append("' THEN ");
			buf.append(typesMap.get(mysqlTypeName));

			if (mysqlTypeName.equalsIgnoreCase("DOUBLE")
					|| mysqlTypeName.equalsIgnoreCase("FLOAT")
					|| mysqlTypeName.equalsIgnoreCase("DECIMAL")
					|| mysqlTypeName.equalsIgnoreCase("NUMERIC")) {
				buf.append(" WHEN ");
				buf.append(mysqlTypeColumnName);
				buf.append("='");
				buf.append(mysqlTypeName);
				buf.append(" unsigned' THEN ");
				buf.append(typesMap.get(mysqlTypeName));
			}
		}

		buf.append(" ELSE ");
		buf.append(Types.OTHER);
		buf.append(" END ");

	}

	public static final String SQL_STATE_BASE_TABLE_NOT_FOUND = "S0002"; //$NON-NLS-1$

	public static final String SQL_STATE_BASE_TABLE_OR_VIEW_ALREADY_EXISTS = "S0001"; //$NON-NLS-1$

	public static final String SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND = "42S02"; //$NON-NLS-1$

	public static final String SQL_STATE_COLUMN_ALREADY_EXISTS = "S0021"; //$NON-NLS-1$

	public static final String SQL_STATE_COLUMN_NOT_FOUND = "S0022"; //$NON-NLS-1$

	public static final String SQL_STATE_COMMUNICATION_LINK_FAILURE = "08S01"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_FAIL_DURING_TX = "08007"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_IN_USE = "08002"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_NOT_OPEN = "08003"; //$NON-NLS-1$

	public static final String SQL_STATE_CONNECTION_REJECTED = "08004"; //$NON-NLS-1$

	public static final String SQL_STATE_DATE_TRUNCATED = "01004"; //$NON-NLS-1$

	public static final String SQL_STATE_DATETIME_FIELD_OVERFLOW = "22008"; //$NON-NLS-1$

	public static final String SQL_STATE_DEADLOCK = "41000"; //$NON-NLS-1$

	public static final String SQL_STATE_DISCONNECT_ERROR = "01002"; //$NON-NLS-1$

	public static final String SQL_STATE_DIVISION_BY_ZERO = "22012"; //$NON-NLS-1$

	public static final String SQL_STATE_DRIVER_NOT_CAPABLE = "S1C00"; //$NON-NLS-1$

	public static final String SQL_STATE_ERROR_IN_ROW = "01S01"; //$NON-NLS-1$

	public static final String SQL_STATE_GENERAL_ERROR = "S1000"; //$NON-NLS-1$

	public static final String SQL_STATE_ILLEGAL_ARGUMENT = "S1009"; //$NON-NLS-1$

	public static final String SQL_STATE_INDEX_ALREADY_EXISTS = "S0011"; //$NON-NLS-1$

	public static final String SQL_STATE_INDEX_NOT_FOUND = "S0012"; //$NON-NLS-1$

	public static final String SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST = "21S01"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_AUTH_SPEC = "28000"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST = "22018"; // $NON_NLS
																						// -
																						// 1
																						// $

	public static final String SQL_STATE_INVALID_COLUMN_NUMBER = "S1002"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_CONNECTION_ATTRIBUTE = "01S00"; //$NON-NLS-1$

	public static final String SQL_STATE_MEMORY_ALLOCATION_FAILURE = "S1001"; //$NON-NLS-1$

	public static final String SQL_STATE_MORE_THAN_ONE_ROW_UPDATED_OR_DELETED = "01S04"; //$NON-NLS-1$

	public static final String SQL_STATE_NO_DEFAULT_FOR_COLUMN = "S0023"; //$NON-NLS-1$

	public static final String SQL_STATE_NO_ROWS_UPDATED_OR_DELETED = "01S03"; //$NON-NLS-1$

	public static final String SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE = "22003"; //$NON-NLS-1$

	public static final String SQL_STATE_PRIVILEGE_NOT_REVOKED = "01006"; //$NON-NLS-1$

	public static final String SQL_STATE_SYNTAX_ERROR = "42000"; //$NON-NLS-1$

	public static final String SQL_STATE_TIMEOUT_EXPIRED = "S1T00"; //$NON-NLS-1$

	public static final String SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN = "08007"; // $NON_NLS
																					// -
																					// 1
																					// $

	public static final String SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE = "08001"; //$NON-NLS-1$

	public static final String SQL_STATE_WRONG_NO_OF_PARAMETERS = "07001"; //$NON-NLS-1$

	public static final String SQL_STATE_INVALID_TRANSACTION_TERMINATION = "2D000"; // $NON_NLS
																					// -
																					// 1
																					// $
}
