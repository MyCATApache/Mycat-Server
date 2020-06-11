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
import java.sql.*;

/**
 * @author Junwen Chen
 **/
public enum TextConvertorImpl implements TextConvertor {
  INSTANCE;

  @Override
  public byte[] convertBigDecimal(BigDecimal v) {
    return v.toPlainString().getBytes();
  }

  @Override
  public byte[] convertBoolean(boolean v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertByte(byte v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertShort(short v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertInteger(int v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertLong(long v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertFloat(float v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertDouble(double v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertBytes(byte[] v) {
    return v;
  }

  @Override
  public byte[] convertDate(Date v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertTime(Time v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertTimeStamp(Timestamp v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertBlob(Blob v) {
    try {
      return v.getBytes(0, (int) v.length());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] convertClob(Clob v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertObject(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof byte[]) {
      return (byte[]) v;
    }
    return v.toString().getBytes();
  }
}