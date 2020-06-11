/**
 * Copyright (C) <2020>  <chen junwen>
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

package io.mycat.defCommand;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Objects;


/**
 *
 * chen junwen
 *
 * a iterator,like jdbc result set
 */
public abstract class AbstractObjectRowIterator implements RowBaseIterator {

    protected Object[] currentRow;
    protected boolean wasNull;

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return Objects.toString(o);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return false;
        return (Boolean) o;
    }

    @Override
    public byte getByte(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Byte) o;
    }

    @Override
    public short getShort(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Short) o;
    }

    @Override
    public int getInt(int columnIndex) {
        Number o = (Number) getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return o.intValue();
    }

    @Override
    public long getLong(int columnIndex) {
        try {
            Number o = (Number) getObject(currentRow[columnIndex - 1]);
            if (wasNull) return 0;
            return ((Number) o).longValue();
        }catch (ClassCastException e){
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) {
        Number o = (Number)getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return ((Number) o).floatValue();
    }

    @Override
    public double getDouble(int columnIndex) {
        Number o = (Number)getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return ((Number) o).doubleValue();
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (byte[]) o;
    }

    @Override
    public Date getDate(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        if(o instanceof Number){
            return new Date(((Number) o).longValue());
        }
        return (Date) o;
    }

    @Override
    public Time getTime(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        if(o instanceof Number){
            return new Time(((Number) o).longValue());
        }
        return (Time) o;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        if(o instanceof Number){
            return new Timestamp(((Number) o).longValue());
        }
        return (Timestamp) o;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (InputStream) o;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (InputStream) o;
    }

    @Override
    public Object getObject(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        return o;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (BigDecimal) o;
    }

    private Object getObject(Object o1) {
        Object o = o1;
        wasNull = null == o;
        return o;
    }
}