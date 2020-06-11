/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.server;

/**
 * @author jamie12221
 *  date 2019-05-07 13:58
 *
 * 字段包
 **/
public interface ColumnDefPacket {

  byte[] DEFAULT_CATALOG = "def".getBytes();

  /**
   * buffer.skipInReading(4);
   * byte[] catalog = buffer.readLenencStringBytes();
   * byte[] schema = buffer.readLenencStringBytes();
   * byte[] table = buffer.readLenencStringBytes();
   * byte[] orgTable = buffer.readLenencStringBytes();
   * byte[] name = buffer.readLenencStringBytes();
   * byte[] orgName = buffer.readLenencStringBytes();
   * byte nextLength = buffer.readByte();
   * int charsetSet = (int) buffer.readFixInt(2);
   * int columnLength = (int) buffer.readFixInt(4);
   * byte type = (byte) (buffer.readByte() &amp; 0xff);
   * int flags = (int) buffer.readFixInt(2);
   * byte decimals = buffer.readByte();
   * <p>
   * buffer.skipInReading(2);
   * if (buffer.packetReadStartIndex() != endPos) {
   * int i = buffer.readLenencInt();
   * byte[] defaultValues = buffer.readFixStringBytes(i);
   * }
   *
   * @return
   */
  byte[] getColumnCatalog();

  void setColumnCatalog(byte[] catalog);

  byte[] getColumnSchema();

  void setColumnSchema(byte[] schema);

  byte[] getColumnTable();

  void setColumnTable(byte[] table);

  byte[] getColumnOrgTable();

  void setColumnOrgTable(byte[] orgTable);

  byte[] getColumnName();

  void setColumnName(byte[] name);

  default String getColumnNameString() {
    return new String(getColumnName());
  }

  byte[] getColumnOrgName();

  void setColumnOrgName(byte[] orgName);

  int getColumnNextLength();

  void setColumnNextLength(int nextLength);

  int getColumnCharsetSet();

  void setColumnCharsetSet(int charsetSet);

  int getColumnLength();

  void setColumnLength(int columnLength);

  int getColumnType();

  void setColumnType(int type);

  int getColumnFlags();

  void setColumnFlags(int flags);

  byte getColumnDecimals();

  void setColumnDecimals(byte decimals);

  byte[] getColumnDefaultValues();

  void setColumnDefaultValues(byte[] defaultValues);

  String toString();

}
