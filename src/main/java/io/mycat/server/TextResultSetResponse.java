/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.server;


import io.mycat.defCommand.MycatRowMetaData;
import io.mycat.defCommand.RowBaseIterator;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class TextResultSetResponse extends AbstractMycatResultSetResponse {
    byte[][] row;

    public TextResultSetResponse(RowBaseIterator iterator) {
        super(iterator);
        row = new byte[iterator.getMetaData().getColumnCount()/**/][];
    }

    @Override
    public Iterator<byte[]> rowIterator() {
        final RowBaseIterator rowBaseIterator = iterator;
        final MycatRowMetaData mycatRowMetaData = rowBaseIterator.getMetaData();
        final TextConvertor convertor = TextConvertorImpl.INSTANCE;
        final int columnCount = mycatRowMetaData.getColumnCount();

        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return rowBaseIterator.next();
            }

            @Override
            public byte[] next() {

                for (int columnIndex = 1, rowIndex = 0; rowIndex < columnCount; columnIndex++, rowIndex++) {
                    int columnType = mycatRowMetaData.getColumnType(columnIndex);
                    row[rowIndex] = getValue(rowBaseIterator, convertor, columnIndex, columnType);
                }
                return null;//不需要返回
            }
        };
    }

    private byte[] getValue(RowBaseIterator rowBaseIterator, TextConvertor convertor, int columnIndex,
                            int columnType) {
        Object object = rowBaseIterator.getObject(columnIndex);
        if ( object == null){
            return null;
        }
        if (object instanceof  byte[]){
            return (byte[])object;
        }
        return Objects.toString(object).getBytes();
    }

    @Override
    public void close() throws IOException {
        this.iterator.close();
    }

  public byte[][] getRow() {
    return row;
  }
}