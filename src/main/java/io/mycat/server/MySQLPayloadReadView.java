/**
 * Copyright (C) <2019>  <chen junwen>
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

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * 报文读视图
 **/
public interface MySQLPayloadReadView<T extends MySQLPayloadReadView<T>> {

  int length();

  long readFixInt(int length);

  int readLenencInt();

  String readFixString(int length);

  String readLenencString();

  byte[] readLenencStringBytes();

  byte[] readNULStringBytes();

  String readNULString();

  byte[] readEOFStringBytes();

  String readEOFString();

  byte[] readBytes(int length);

  byte[] readFixStringBytes(int length);

  byte readByte();

  byte[] readLenencBytes();

  long readLong();

  double readDouble();

  void reset();

  void skipInReading(int i);

  boolean readFinished();
}
