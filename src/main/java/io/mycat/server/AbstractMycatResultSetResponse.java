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


import io.mycat.defCommand.RowBaseIterator;
import io.mycat.server.resultset.MycatResultSetResponse;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Junwen Chen
 **/
public abstract class AbstractMycatResultSetResponse implements MycatResultSetResponse {

  protected final RowBaseIterator iterator;

  public AbstractMycatResultSetResponse(RowBaseIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public int columnCount() {
    return iterator.getMetaData().getColumnCount();
  }

  @Override
  public Iterator<byte[]> columnDefIterator() {
    return new Iterator<byte[]>() {
      final int count = columnCount();
      int index = 1;

      @Override
      public boolean hasNext() {
        return index <= count;
      }

      @Override
      public byte[] next() {
        return MySQLPacketUtil
            .generateColumnDefPayload(
                iterator.getMetaData(),
                index++);
      }
    };
  }

  @Override
  public void close() throws IOException {
    iterator.close();
  }
}