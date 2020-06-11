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
 * 报文写视图
 **/
public interface MySQLPayloadWriteView<T extends MySQLPayloadWriteView<T>> {

  T writeLong(long l);

  T writeFixInt(int length, long val);

  T writeLenencInt(long val);

  T writeFixString(String val);

  T writeFixString(byte[] val);

  T writeLenencBytesWithNullable(byte[] bytes);

  T writeLenencString(byte[] bytes);

  T writeLenencString(String val);

  T writeBytes(byte[] bytes);
  T writeBytes(byte[] bytes, int offset, int length);

  T writeNULString(String val);

  T writeNULString(byte[] vals);

  T writeEOFString(String val);

  T writeEOFStringBytes(byte[] bytes);

  T writeLenencBytes(byte[] bytes);

  T writeLenencBytes(byte[] bytes, byte[] nullValue);

  T writeByte(byte val);

  default T writeByte(int val) {
    return writeByte((byte) val);
  }

  T writeReserved(int length);

  T writeDouble(double d);
}
