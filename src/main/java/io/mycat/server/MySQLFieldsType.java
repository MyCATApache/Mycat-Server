/**
 * Copyright (C) <2019>  <mycat>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.server;

import java.sql.Types;
import java.util.HashMap;


/**
 * 字段类型及标识定义
 *
 * @author mycat
 * @author chen junwen
 */
public class MySQLFieldsType {

  /**
   * field data type
   */
  public static final int FIELD_TYPE_DECIMAL = 0;
  public static final int FIELD_TYPE_TINY = 1;
  public static final int FIELD_TYPE_SHORT = 2;
  public static final int FIELD_TYPE_LONG = 3;
  public static final int FIELD_TYPE_FLOAT = 4;
  public static final int FIELD_TYPE_DOUBLE = 5;
  public static final int FIELD_TYPE_NULL = 6;
  public static final int FIELD_TYPE_TIMESTAMP = 7;
  public static final int FIELD_TYPE_LONGLONG = 8;
  public static final int FIELD_TYPE_INT24 = 9;
  public static final int FIELD_TYPE_DATE = 10;
  public static final int FIELD_TYPE_TIME = 11;
  public static final int FIELD_TYPE_DATETIME = 12;
  public static final int FIELD_TYPE_YEAR = 13;
  public static final int FIELD_TYPE_NEWDATE = 14;
  public static final int FIELD_TYPE_VARCHAR = 15;
  public static final int FIELD_TYPE_BIT = 16;
  public static final int FIELD_TYPE_NEW_DECIMAL = 246;
  public static final int FIELD_TYPE_ENUM = 247;
  public static final int FIELD_TYPE_SET = 248;
  public static final int FIELD_TYPE_TINY_BLOB = 249;
  public static final int FIELD_TYPE_MEDIUM_BLOB = 250;
  public static final int FIELD_TYPE_LONG_BLOB = 251;
  public static final int FIELD_TYPE_BLOB = 252;
  public static final int FIELD_TYPE_VAR_STRING = 253;
  public static final int FIELD_TYPE_STRING = 254;
  public static final int FIELD_TYPE_GEOMETRY = 255;

  /**
   * field flag
   */
  public static final int NOT_NULL_FLAG = 0x0001;
  public static final int PRI_KEY_FLAG = 0x0002;
  public static final int UNIQUE_KEY_FLAG = 0x0004;
  public static final int MULTIPLE_KEY_FLAG = 0x0008;
  public static final int BLOB_FLAG = 0x0010;
  public static final int UNSIGNED_FLAG = 0x0020;
  public static final int ZEROFILL_FLAG = 0x0040;
  public static final int BINARY_FLAG = 0x0080;
  public static final int ENUM_FLAG = 0x0100;
  public static final int AUTO_INCREMENT_FLAG = 0x0200;
  public static final int TIMESTAMP_FLAG = 0x0400;
  public static final int SET_FLAG = 0x0800;

  final static HashMap<Byte, Integer> JDBC2MYSQLMAP = new HashMap<>();

  static {
    initPut(Types.BIT, FIELD_TYPE_BIT);
    initPut(Types.TINYINT, FIELD_TYPE_TINY);
    initPut(Types.SMALLINT, FIELD_TYPE_SHORT);
    initPut(Types.INTEGER, FIELD_TYPE_LONG);
    initPut(Types.BIGINT, FIELD_TYPE_LONGLONG);
    initPut(Types.FLOAT, FIELD_TYPE_FLOAT);
    initPut(Types.REAL, FIELD_TYPE_FLOAT);
    initPut(Types.DOUBLE, FIELD_TYPE_DOUBLE);
    initPut(Types.NUMERIC, FIELD_TYPE_DECIMAL);
    initPut(Types.DECIMAL, FIELD_TYPE_DECIMAL);
    initPut(Types.CHAR, FIELD_TYPE_STRING);
    initPut(Types.VARCHAR, FIELD_TYPE_STRING);
    initPut(Types.LONGVARCHAR, FIELD_TYPE_STRING);
    initPut(Types.DATE, FIELD_TYPE_DATE);
    initPut(Types.TIME, FIELD_TYPE_TIME);
    initPut(Types.TIMESTAMP, FIELD_TYPE_TIMESTAMP);
    initPut(Types.BINARY, FIELD_TYPE_STRING);
    initPut(Types.VARBINARY, FIELD_TYPE_STRING);
    initPut(Types.LONGVARBINARY, FIELD_TYPE_STRING);
    initPut(Types.NULL, FIELD_TYPE_NULL);
    initPut(Types.BLOB, FIELD_TYPE_BLOB);
    initPut(Types.OTHER, FIELD_TYPE_STRING);
  }

  public static int fromJdbcType(int jdbcType) {
    return JDBC2MYSQLMAP.get((byte) jdbcType);
  }

  private static void initPut(int jdbcType, int mysqlType) {
    JDBC2MYSQLMAP.put((byte) jdbcType, mysqlType);
  }
}