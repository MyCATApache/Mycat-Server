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
package io.mycat.server.resultset;

import java.io.IOException;
import java.util.Iterator;

public interface MycatResultSet extends MycatResultSetResponse<byte[]>  {

  void addColumnDef(int index, String database, String table,
                    String originalTable,
                    String columnName, String orgName, int type,
                    int columnFlags,
                    int columnDecimals, int length);

  void addColumnDef(int index, String columnName, int type);

  int columnCount();

  Iterator<byte[]> columnDefIterator();

  void addTextRowPayload(String... row);

  void addTextRowPayload(byte[]... row);

  void addObjectRowPayload(Object[]... row);

  Iterator<byte[]> rowIterator();

  void close() throws IOException;
}