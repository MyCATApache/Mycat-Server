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
 *  date 2019-05-05 16:22
 *
 * MySQLPacketSplitter 默认实现
 **/
public class PacketSplitterImpl implements MySQLPacketSplitter {

  int totalSize;
  int currentPacketLen;
  int offset;

  @Override
  public int getTotalSizeInPacketSplitter() {
    return totalSize;
  }

  @Override
  public void setTotalSizeInPacketSplitter(int totalSize) {
    this.totalSize = totalSize;
  }

  @Override
  public int getPacketLenInPacketSplitter() {
    return currentPacketLen;
  }

  @Override
  public void setPacketLenInPacketSplitter(int currentPacketLen) {
    this.currentPacketLen = currentPacketLen;
  }

  @Override
  public void setOffsetInPacketSplitter(int offset) {
    this.offset  = offset;
  }


  @Override
  public int getOffsetInPacketSplitter() {
    return offset;
  }
}
