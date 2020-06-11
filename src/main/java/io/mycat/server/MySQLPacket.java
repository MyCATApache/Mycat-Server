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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @author jamie12221
 *  date 2019-05-07 13:58
 *
 * mycat报文基础读写
 *
 * create set方法族 不会修改mysql packet,proxybuffer,bytebuffer内部状态 read write方法族 修改内部状态
 **/
public interface MySQLPacket<T extends ProxyBuffer> extends MySQLPayloadReadView,
                                                                MySQLPayloadWriteView {

  byte[] EMPTY_BYTE_ARRAY = new byte[]{};

  /**
   * 在不压缩情况下,报文头部的长度
   */
  static int getPacketHeaderSize() {
    return 4;
  }

  T currentBuffer();

  /**
   * 帮助类 需要主要的是,开始读取的下标是buffer的position
   *
   * @param buffer buffer
   * @param length 长度
   */
  static int readInt(ByteBuffer buffer, int length) {
    int rv = 0;
    for (int i = 0; i < length; i++) {
      byte b = buffer.get();
      rv |= (((long) b) & 0xFF) << (i * 8);
    }
    return rv;
  }

  /**
   * 获取lenenc占用的字节长度
   *
   * @param lenenc 值
   * @return 长度
   */
  static int getLenencLength(int lenenc) {
    if (lenenc < 251) {
      return 1;
    } else if (lenenc >= 251 && lenenc < (1 << 16)) {
      return 3;
    } else if (lenenc >= (1 << 16) && lenenc < (1 << 24)) {
      return 4;
    } else {
      return 9;
    }
  }

  default boolean isErrorPacket() {
    return (getByte(packetReadStartIndex()) & 0xff) == 0xff;
  }

  static void writeFixIntByteBuffer(ByteBuffer buffer, int length, long val) {
    for (int i = 0; i < length; i++) {
      byte b = (byte) ((val >>> (i * 8)) & 0xFF);
      buffer.put(b);
    }
  }

  /**
   * 清除 mysql packet的状态,清理bytebuffer
   */
  void reset();

  /**
   * read方法开始读取的位置
   */
  int packetReadStartIndex();

  int packetReadStartIndex(int index);

  /**
   * read方法读取结束的位置
   */
  int packetReadEndIndex();

  int packetReadEndIndex(int endPos);

  /**
   * packetStartIndex向前移动的长度
   */
  default int packetReadStartIndexAdd(int len) {
    return packetReadStartIndex(packetReadStartIndex() + len);
  }

  int packetWriteIndex();
//
//    public T appendInReading(T packet);
//    public T appendInWriting(T packet);

  int packetWriteIndex(int index);

  /**
   * packetWriteIndex向前移动的长度,实际上就是bytebuffer的position
   */
  default int packetWriteIndexAdd(int len) {
    return packetWriteIndex(packetWriteIndex() + len);
  }

  /**
   * 不做任务修改,把bytes写入bytebuffer
   */
  default MySQLPacket writeBytes(byte[] bytes) {
    this.writeBytes(bytes.length, bytes);
    return this;
  }

  default long readFixInt(int length) {
    long val = readInt(packetReadStartIndex(), length);
    packetReadStartIndexAdd(length);
    return val;
  }

  default long getFixInt(int index, int length) {
    return readInt(index, length);
  }

  default int readLenencInt() {
    int index = packetReadStartIndex();
    long len = readInt(index, 1) & 0xff;
    if (len < 251) {
      packetReadStartIndexAdd(1);
      return readInt(index, 1);
    } else if (len == 0xfc) {
      packetReadStartIndexAdd(3);
      return readInt(index + 1, 2);
    } else if (len == 0xfd) {
      packetReadStartIndexAdd(4);
      return readInt(index + 1, 3);
    } else {
      packetReadStartIndexAdd(9);
      return readInt(index + 1, 8);
    }
  }

  default int readInt(int index, int length) {
    currentBuffer().position(index);
    int rv = 0;
    for (int i = 0; i < length; i++) {
      byte b = currentBuffer().get();
      rv |= (((long) b) & 0xFF) << (i * 8);
    }
    return rv;
  }

  default byte[] getBytes(int index, int length) {
    currentBuffer().position(index);
    byte[] bytes = new byte[length];
    currentBuffer().get(bytes);
    return bytes;
  }

  default byte getByte(int index) {
    return currentBuffer().get(index);
  }

  default String getFixString(int index, int length) {
    byte[] bytes = getBytes(index, length);
    return new String(bytes);
  }

  default byte[] readFixStringBytes(int length) {
    byte[] bytes = getBytes(packetReadStartIndex(), length);
    packetReadStartIndexAdd(length);
    return bytes;
  }

  default String readFixString(int length) {
    byte[] bytes = getBytes(packetReadStartIndex(), length);
    packetReadStartIndexAdd(length);
    return new String(bytes);
  }

  default String getLenencString(int index) {
    int strLen = (int) getLenencInt(index);
    int lenencLen = getLenencLength(strLen);
    byte[] bytes = getBytes(index + lenencLen, strLen);
    return new String(bytes);
  }

  default String readLenencString() {
    int startIndex = packetReadStartIndex();
    byte[] bytes = readLenencStringBytes();
    if (bytes == null) {
      return null;
    } else {
      return new String(bytes);
    }
  }

  default byte[] readLenencStringBytes() {
    return readLenencBytes();
  }

  default byte[] getNULStringBytes(int index) {
    int strLength = 0;
    int scanIndex = index;
    int length = packetReadEndIndex();
    while (scanIndex < length) {
      if (getByte(scanIndex++) == 0) {
        break;
      }
      strLength++;
    }
    return getBytes(index, strLength);
  }

  default byte[] readNULStringBytes() {
    byte[] rv = getNULStringBytes(packetReadStartIndex());
    packetReadStartIndexAdd(rv.length + 1);
    return rv;
  }

  default String readNULString() {
    return new String(readNULStringBytes());
  }

  default byte[] getEOFStringBytes(int index) {
    int strLength = packetReadEndIndex() - index;
    if (strLength <= 0) {
      return new byte[0];
    }
    return getBytes(index, strLength);
  }

  default byte[] readEOFStringBytes() {
    byte[] rv = getEOFStringBytes(packetReadStartIndex());
    packetReadStartIndexAdd(rv.length);
    return rv;
  }

  default String getEOFString(int index) {
    return new String(getEOFStringBytes(index));
  }

  default String readEOFString() {
    return new String(readEOFStringBytes());
  }

  default MySQLPacket putFixInt(int index, int length, long val) {
    int index0 = index;
    for (int i = 0; i < length; i++) {
      byte b = (byte) ((val >>> (i * 8)) & 0xFF);
      putByte(index0++, b);
    }
    return this;
  }

  default MySQLPacket writeFixInt(int length, long val) {
    putFixInt(packetWriteIndex(), length, val);
    return this;
  }

  default int putLenencIntReLenencLen(int index, long val) {
    if (val < 251) {
      putByte(index, (byte) val);
      return 1;
    } else if (val >= 251 && val < (1 << 16)) {
      putByte(index, (byte) 0xfc);
      putFixInt(index + 1, 2, val);
      return 3;
    } else if (val >= (1 << 16) && val < (1 << 24)) {
      putByte(index, (byte) 0xfd);
      putFixInt(index + 1, 3, val);
      return 4;
    } else {
      putByte(index, (byte) 0xfe);
      putFixInt(index + 1, 8, val);
      return 9;
    }
  }

  default MySQLPacket writeLenencInt(long val) {
    if (val < 251) {
      putByte(packetWriteIndexAdd(1), (byte) val);
    } else if (val >= 251 && val < (1 << 16)) {
      putByte(packetWriteIndexAdd(1), (byte) 0xfc);
      putFixInt(packetWriteIndex(), 2, val);
      packetWriteIndexAdd(2);
    } else if (val >= (1 << 16) && val < (1 << 24)) {
      putByte(packetWriteIndexAdd(1), (byte) 0xfd);
      putFixInt(packetWriteIndex(), 3, val);
      packetWriteIndexAdd(3);
    } else {
      putByte(packetWriteIndexAdd(1), (byte) 0xfe);
      putFixInt(packetWriteIndex(), 8, val);
      packetWriteIndexAdd(8);
    }
    return this;
  }

  default MySQLPacket putFixString(int index, String val) {
    putBytes(index, val.getBytes());
    return this;
  }

  default MySQLPacket putFixString(int index, byte[] val) {
    putBytes(index, val);
    return this;
  }

  default MySQLPacket writeFixString(String val) {
    byte[] bytes = val.getBytes();
    putBytes(packetWriteIndex(), bytes);
    return this;
  }

  default MySQLPacket writeFixString(byte[] val) {
    putBytes(packetWriteIndex(), val);
    return this;
  }

  default MySQLPacket putLenencString(int index, String val) {
    byte[] bytes = val.getBytes();
    int lenencLen = this.putLenencIntReLenencLen(index, bytes.length);
    this.putFixString(index + lenencLen, bytes);
    return this;
  }

  default MySQLPacket putLenencString(int index, byte[] val) {
    int lenencLen = this.putLenencIntReLenencLen(index, val.length);
    this.putFixString(index + lenencLen, val);
    return this;
  }

  default MySQLPayloadWriteView writeLenencBytesWithNullable(byte[] bytes) {
    byte nullVal = 0;
    if (bytes == null) {
      currentBuffer().put(nullVal);
    } else {
      writeLenencBytes(bytes);
    }
    return this;
  }

  default MySQLPacket writeLenencString(byte[] bytes) {
    putLenencString(packetWriteIndex(), bytes);
    int lenencLen = getLenencLength(bytes.length);
    return this;
  }

  default MySQLPacket writeLenencString(String val) {
    return writeLenencString(val.getBytes());
  }

  default MySQLPacket putBytes(int index, byte[] bytes) {
    putBytes(index, bytes, bytes.length);
    return this;
  }

  default MySQLPacket writeBytes(byte[] bytes, int offset, int length) {
    currentBuffer().position(this.packetWriteIndex());
    currentBuffer().put(bytes, offset, length);
    return this;
  }

  /**
   * 该方法直接对bytebuffer进行操作
   */
  default MySQLPacket writeBytes(int index, byte[] bytes, int offset, int length) {
    currentBuffer().position(index);
    currentBuffer().put(bytes, offset, length);
    return this;
  }

  default MySQLPacket putBytes(int index, byte[] bytes, int length) {
    currentBuffer().position(index);
    currentBuffer().put(bytes, 0, length);
    return this;
  }

  default MySQLPacket putByte(int index, byte val) {
    currentBuffer().position(index);
    currentBuffer().put(val);
    return this;
  }

  default MySQLPacket putNULString(int index, String val) {
    byte[] bytes = val.getBytes();
    putFixString(index, bytes);
    putByte(bytes.length + index, (byte) 0);
    return this;
  }

  default MySQLPacket putNULString(int index, byte[] bytes) {
    putFixString(index, bytes);
    putByte(bytes.length + index, (byte) 0);
    return this;
  }

  default MySQLPacket writeNULString(String val) {
    byte[] bytes = val.getBytes();
    putNULString(packetWriteIndex(), bytes);
    return this;
  }

  default MySQLPacket writeNULString(byte[] vals) {
    putNULString(packetWriteIndex(), vals);

    return this;
  }

  default MySQLPacket writeEOFString(String val) {
    byte[] bytes = val.getBytes();
    putFixString(packetWriteIndex(), bytes);

    return this;
  }

  default MySQLPacket writeEOFStringBytes(byte[] bytes) {
    putFixString(packetWriteIndex(), bytes);
    return this;
  }

  default byte[] readBytes(int length) {
    byte[] bytes = this.getBytes(packetReadStartIndex(), length);
    packetReadStartIndexAdd(length);
    return bytes;
  }

  default MySQLPacket writeBytes(int length, byte[] bytes) {
    this.putBytes(packetWriteIndex(), bytes, length);
    return this;
  }


  default MySQLPacket writeLenencBytes(byte[] bytes) {
    int offset = this.putLenencIntReLenencLen(packetWriteIndex(), bytes.length);
    putBytes(packetWriteIndex() + offset, bytes);
    return this;
  }

  default MySQLPacket writeLenencBytes(byte[] bytes, byte[] nullValue) {
    if (bytes == null) {
      return writeLenencBytes(nullValue);
    } else {
      return writeLenencBytes(bytes);
    }
  }


  default MySQLPacket writeByte(byte val) {
    this.putByte(packetWriteIndex(), val);
    return this;
  }

  default MySQLPacket writeReserved(int length) {
    int i1 = packetWriteIndex();
    for (int i = 0; i < length; i++) {
      this.writeByte((byte) 0);
    }
    return this;
  }

  default byte readByte() {
    byte val = getByte(packetReadStartIndex());
    packetReadStartIndexAdd(1);
    return val;
  }

//  default byte[] getLenencBytes(int index) {
//    int len = (int) getLenencInt(index);
//    return getBytes(index + getLenencLength(len), len);
//  }

  default int skipLenencBytes(int index) {
    int len = (int) getLenencInt(packetReadStartIndex(index));
    byte[] bytes = null;
    if ((len & 0xff) == 0xfb) {
      bytes = EMPTY_BYTE_ARRAY;
      packetReadStartIndexAdd(1);
      return packetReadStartIndex();
    } else {
    }
    packetReadStartIndexAdd(getLenencLength(len) + len);
    return packetReadStartIndex();
  }

  default long getLenencInt(int index) {
    long len = readInt(index, 1) & 0xff;
    if (len == 0xfc) {
      return readInt(index + 1, 2);
    } else if (len == 0xfd) {
      return readInt(index + 1, 3);
    } else if (len == 0xfe) {
      return readInt(index + 1, 8);
    } else if (len == 0xfb) {
      return len;
    } else {
      return len;
    }
  }

  default byte[] readLenencBytes() {
    int len = (int) getLenencInt(packetReadStartIndex());
    byte[] bytes = null;
    if ((len & 0xff) == 0xfb) {
      bytes = EMPTY_BYTE_ARRAY;
      packetReadStartIndexAdd(1);
      return null;
    } else {
      bytes = getBytes(packetReadStartIndex() + getLenencLength(len), len);
    }
    packetReadStartIndexAdd(getLenencLength(len) + len);
    return bytes;
  }

  default MySQLPacket putLenencBytes(int index, byte[] bytes) {
    int offset = this.putLenencIntReLenencLen(index, bytes.length);
    putBytes(index + offset, bytes);
    return this;
  }

  /**
   * packetReadStartIndex() == packetReadEndIndex()
   */
  default boolean readFinished() {
    return packetReadStartIndex() == packetReadEndIndex();
  }

  default void skipInReading(int i) {
    packetReadStartIndexAdd(i);
  }

  default void writeSkipInWriting(int i) {
    packetWriteIndex(packetWriteIndex() + i);
  }

  void writeFloat(float f);

  float readFloat();

  MySQLPacket writeLong(long l);

  long readLong();

  MySQLPacket writeDouble(double d);

  double readDouble();

  void writeShort(short o);

  /**
   * time读取 binaryResultSet
   */
  default Time readTime() {
    boolean negative;
    int days;
    int hours;
    int minutes;
    int seconds;
    int nanos;
    int length = readByte();
    if (length == 0) {
      negative = false;
      days = 0;
      hours = 0;
      minutes = 0;
      seconds = 0;
      nanos = 0;
    } else if (length == 8) {
      negative = readByte() == 1;
      days = (int) readFixInt(4);
      hours = readByte() & 0xff;

      minutes = readByte() & 0xff;
      seconds = readByte() & 0xff;
      nanos = 0;
    } else if (length == 12) {

      negative = readByte() == 1;
      days = (int) readFixInt(4);

      hours = readByte() & 0xff;
      minutes = readByte() & 0xff;
      seconds = readByte() & 0xff;
      byte[] bytes = readBytes(4);
      int offset = 0;
      nanos = 1000 * (bytes[offset + 1] & 0xff) | ((bytes[offset + 2] & 0xff) << 8) | (
          (bytes[offset + 3] & 0xff) << 16)
                  | ((bytes[offset + 4] & 0xff) << 24);
    } else {
      throw new RuntimeException("UNKOWN FORMAT");
    }
    if (negative) {
      days *= -1;
    }
    return Time.valueOf(LocalTime.of(days * 24 + hours, minutes, seconds, nanos));
  }

  /**
   * date 读取 BinaryResultSet
   */
  default java.util.Date readDate() {
    byte length = readByte();
    if (length == 0) {
      return java.sql.Date.valueOf(LocalDate.of(0, 0, 0));
    } else if (length == 4) {
      return java.sql.Date.valueOf(LocalDate.of((int) readFixInt(2), 0, 0));
    }
    int year = (int) readFixInt(2);
    int month = readByte() & 0xff;
    int date = readByte() & 0xff;
    int hour = readByte() & 0xff;
    int minute = readByte() & 0xff;
    int second = readByte() & 0xff;
    if (length == 7) {
      return java.sql.Timestamp.valueOf(LocalDateTime.of(year, month, date, hour, minute, second));
    }

    if (length == 11) {
      int nanos;
      byte[] bytes = readBytes(4);
      int offset = 0;
      nanos = 1000 * (bytes[offset + 1] & 0xff) | ((bytes[offset + 2] & 0xff) << 8) | (
          (bytes[offset + 3] & 0xff) << 16)
                  | ((bytes[offset + 4] & 0xff) << 24);
      return java.sql.Timestamp
                 .valueOf(LocalDateTime.of(year, month, date, hour, minute, second, nanos));
    } else {

      throw new RuntimeException("UNKOWN FORMAT");
    }
  }

  /**
   * Decimal 读取 BinaryResultSet
   */
  default BigDecimal readBigDecimal() {
    String src = readLenencString();
    return src == null ? null : new BigDecimal(src);
  }

}
