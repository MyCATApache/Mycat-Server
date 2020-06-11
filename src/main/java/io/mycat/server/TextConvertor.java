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
 * @author Junwen Chen
 **/
import java.math.BigDecimal;

public interface TextConvertor {

  byte[] convertBigDecimal(BigDecimal v);

  byte[] convertBoolean(boolean v);

  byte[] convertByte(byte v);

  byte[] convertShort(short v);

  byte[] convertInteger(int v);

  byte[] convertLong(long v);

  byte[] convertFloat(float v);

  byte[] convertDouble(double v);

  byte[] convertBytes(byte[] v);

  byte[] convertDate(java.sql.Date v);

  byte[] convertTime(java.sql.Time v);

  byte[] convertTimeStamp(java.sql.Timestamp v);

  byte[] convertBlob(java.sql.Blob v);

  byte[] convertClob(java.sql.Clob v);

  byte[] convertObject(Object v);
}