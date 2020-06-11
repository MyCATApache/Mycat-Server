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

import io.mycat.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author chen junwen
 *  date 2019-05-09 02:30
 *
 * proxybuffer 用于代理数据交换
 **/
public interface ProxyBuffer {

   static final Logger logger = LoggerFactory.getLogger(ProxyBuffer.class);

  /**
   * 获取ByteBuffer
   */
  ByteBuffer currentByteBuffer();

  /**
   * 获取ByteBuffer的容量
   * @return
   */
  int capacity();

  /**
   * 获取Bytebuffer的位置,即channelWritStarteIndex;
   * @return
   */
  int position();

  int position(int index);

  void writeFloat(float f);

  float readFloat();

  MySQLPacket writeLong(long l);

  long readLong();

  MySQLPacket writeDouble(double d);

  double readDouble();

  byte get();

  byte get(int index);

  ProxyBuffer get(byte[] bytes);

  byte put(byte b);

  void put(byte[] bytes);

  void put(byte[] bytes, int offset, int legnth);

  /**
   * 写入通道时候,数据获取的开始位置
   * @return
   */
  int channelWriteStartIndex();

  /**
   * 写入通道时候,数据获取结束的位置
   * @return
   */
  int channelWriteEndIndex();

  /**
   * 读事件,报文读取类,从该位置开始读取
   * @return
   */
  int channelReadStartIndex();

  /**
   * 读事件,报文读取类,从该位置读取结束
   * @return
   */
  int channelReadEndIndex();

  void channelWriteStartIndex(int index);

  void channelWriteEndIndex(int index);

  void channelReadStartIndex(int index);

  void channelReadEndIndex(int index);

  /**
   * 不改变Proxybuffer的任何下标和数据,扩容bytebuffer
   * @param length
   */
  void expendToLength(int length);

  /**
   * 读事件,从通道读取数据,通道从channelReadStartIndex开始填充数据,直到容量用完
   * @param channel
   * @return
   * @throws IOException
   */
  boolean readFromChannel(SocketChannel channel) throws IOException;

  /**
   * 把Proxybuffer的数据写入通道,从channelWriteStartIndex开始写入
   * @param channel
   * @throws IOException
   */
  void writeToChannel(SocketChannel channel) throws IOException;

  /**
   * 该buffer所在的byteBuffer
   * @return
   */
  BufferPool bufferPool();

  /**
   * 回收bytebuffer,重置状态
   */
  void reset();

  /**
   * 使用buffer池默认分配大小分配buffer
   */
  void newBuffer();

  /**
   * 使用数组构造Proxybuffer,此时buffer处于通读可读可写状态
   * @param bytes
   */
  void newBuffer(byte[] bytes);

  /**
   * 指定大小分配buffer
   * @param len
   */
  void newBuffer(int len);

  /**
   *根据channelReadEndIndex 与 length 比较,扩容
   * @param length
   */
  void expendToLengthIfNeedInReading(int length);

  /**
   *this.buffer.capacity() &lt; length + readStartIndex
   * @param length
   */
  void appendLengthIfInReading(int length);

  /**
   * condition &amp;&amp; readEndIndex &lt; length + readStartIndex
   */
  void appendLengthIfInReading(int length, boolean condition);

  /**
   * readEndIndex &gt; buffer.capacity() * (1.0 / 3)
   */
  void compactInChannelReadingIfNeed();


  /**
   * buffer == null newBuffer()
   * @return
   */
  ProxyBuffer newBufferIfNeed();


  /**
   * channelWriteStartIndex() == channelWriteEndIndex()
   * @return
   */
  default boolean channelWriteFinished() {
    return channelWriteStartIndex() == channelWriteEndIndex();
  }

  /**
   * channelReadStartIndex() == channelReadEndIndex()
   * @return
   */
  default boolean channelReadFinished() {
    return channelReadStartIndex() == channelReadEndIndex();
  }

  /**
   *     buffer.position(channelWriteStartIndex());
   *     buffer.limit(channelWriteEndIndex());
   * @return
   */
  ProxyBuffer applyChannelWritingIndex();

  /**
   *
   * 剪掉指定范围的数据 并更新
   *
   *  this.readStartIndex;
   *  this.readEndIndex;
   *
   *  writeStartIndex writeEndIndex可能会被破坏
   * @param start 剪掉数据开始的下标
   * @param end 剪掉数据结束的下标
   */
  void cutRangeBytesInReading(int start, int end);

  /**
   *   buffer.position(channelReadStartIndex());
   *   buffer.limit(buffer.capacity());
   * @return
   */
  ProxyBuffer applyChannelReadingIndex();

  /**
   *     channelReadStartIndex(channelWriteStartIndex());
   *     channelReadEndIndex(channelWriteEndIndex());
   *
   *     写入的范围应用到读取范围
   */
  void applyChannelWritingIndexForChannelReadingIndex();

  /**
   *     return this.capacity() - readEndIndex;
   *
   *     在读取,报文解析时,检查剩余空间,检查bytebuffer能否容纳整个报文
   * @return
   */
  int remainsInReading();

  void put(ByteBuffer append);
}
