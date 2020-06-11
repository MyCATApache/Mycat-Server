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
package io.mycat.server;


import io.mycat.defCommand.MycatRowMetaData;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.ResultSetMetaData;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author jamie12221 date 2019-05-07 21:23
 *
 * 写入的报文构造工具 注意的是,函数名没有带有packet后缀的,生成的是payload(没有报文头部和拆分报文处理) 带有packet后缀的,会进行报文处理(根据packetid,payload长度进行生成报文)
 **/
public class MySQLPacketUtil {

  private static final byte NULL_MARK = (byte) 251;
  private static final byte EMPTY_MARK = (byte) 0;

  public static final byte[] generateRequest(int head, byte[] data) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(1 + data.length)) {
      writer.write(head);
      writer.write(data);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateComQuery(String sql) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(sql.length() + 5)) {
      writer.write(0x3);
      writer.writeEOFString(sql);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateComQueryPacket(String sql) {
    return generateMySQLPacket(0, generateComQuery(sql));
  }

  public static final byte[] generateComQueryPayload(byte[] sql) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(sql.length + 5)) {
      writer.write(0x3);
      writer.writeEOFStringBytes(sql);
      return writer.toByteArray();
    }
  }
  public static final byte[] generateResetPacket(long statementId) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(5)) {
      writer.write(0x1a);
      writer.writeFixInt(4, statementId);
      return generateMySQLPacket(0, writer.toByteArray());
    }
  }

  public static final byte[] generateClosePacket(long statementId) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(5)) {
      writer.write(0x19);
      writer.writeFixInt(4, statementId);
      return generateMySQLPacket(0, writer.toByteArray());
    }
  }

  public static final byte[] generateExecutePayload(long statementId, byte flags, int numParams,
      byte[] rest) {
    final long iteration = 1;
    try (MySQLPayloadWriter mySQLPacket = new MySQLPayloadWriter(64)) {
      mySQLPacket.writeByte((byte) 0x17);
      mySQLPacket.writeFixInt(4, statementId);
      mySQLPacket.writeByte(flags);
      mySQLPacket.writeFixInt(4, iteration);
      mySQLPacket.writeBytes(rest);
      return mySQLPacket.toByteArray();
    }
  }

  public static final byte[] generateRequestPacket(int head, byte[] data) {
    byte[] bytes = generateRequest(head, data);
    return generateMySQLPacket(0, bytes);
  }

  public static final byte[] generateResultSetCount(int fieldCount) {
    MySQLPayloadWriter writer = new MySQLPayloadWriter(1);
    writer.writeLenencInt(fieldCount);
    return writer.toByteArray();
  }

  public static final byte[] generateColumnDefPayload(String name, int type, int charsetIndex,
                                                      Charset charset) {
    return generateColumnDefPayload("", "", "", name, name, type, 0, 0, charsetIndex, 192, charset);
  }

  public static final byte[] generateColumnDefPayload(ResultSetMetaData metaData, int columnIndex) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(128)) {
      ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl(metaData, columnIndex);
      columnDefPacket.writePayload(writer);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateColumnDefPayload(MycatRowMetaData metaData, int columnIndex) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(128)) {
      ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl(metaData, columnIndex);
      columnDefPacket.writePayload(writer);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateEof(
      int warningCount, int status
  ) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(12)) {
      writer.writeByte(0xfe);
      writer.writeFixInt(2, warningCount);
      writer.writeFixInt(2, status);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateOk(int header,
      int warningCount, int serverStatus, long affectedRows, long lastInsertId,
      boolean isClientProtocol41, boolean isKnowsAboutTransactions,
      boolean sessionVariableTracking, String message
  ) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(12)) {
      writer.writeByte((byte) header);
      writer.writeLenencInt(affectedRows);
      writer.writeLenencInt(lastInsertId);
      if (isClientProtocol41) {
        writer.writeFixInt(2, serverStatus);
        writer.writeFixInt(2, warningCount);
      } else if (isKnowsAboutTransactions) {
        writer.writeFixInt(2, serverStatus);
      }
//      if (sessionVariableTracking) {
//        throw new MycatException("unsupport!!");
//      } else {
//
//      }
        if (message != null) {
            writer.writeBytes(message.getBytes());
        }
      return writer.toByteArray();
    }
  }

  /**
   * 生成简单的错误包
   *
   * @param errno 必须正确设置,否则图形化客户端不会显示
   */

  /**
   * @param errno the error code
   * @param message the error massage
   * @param serverCapabilityFlags server capability
   * @return the data of payload
   */
  public static final byte[] generateError(
          int errno,
          String message, int serverCapabilityFlags
  ) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(64)) {
      ErrorPacketImpl errorPacket = new ErrorPacketImpl();
      errorPacket.setErrorMessage(message.getBytes());
      errorPacket.setErrorCode(errno);
      errorPacket.writePayload(writer, serverCapabilityFlags);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateError(
          String message, int serverCapabilityFlags
  ) {
    return generateError(MySQLErrorCode.ER_UNKNOWN_ERROR, message, serverCapabilityFlags);
  }

  public static final byte[] generateProgressInfoErrorPacket(
      int stage, int maxStage, int progress, byte[] progressInfo
  ) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(64)) {
      ErrorPacketImpl errorPacket = new ErrorPacketImpl();
      errorPacket.setErrorCode(0xFFFF);
      errorPacket.setErrorStage(stage);
      errorPacket.setErrorMaxStage(maxStage);
      errorPacket.setErrorProgress(progress);
      errorPacket.setErrorProgressInfo(progressInfo);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateBinaryRow(
      byte[][] rows) {
    final int columnCount = rows.length;
    final int binaryNullBitMapLength = (columnCount + 7 + 2) / 8;
    byte[] nullMap = new byte[binaryNullBitMapLength];
    final int payloayEstimateMaxSize = generateBinaryRowHeader(rows, nullMap);
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(payloayEstimateMaxSize)) {
      writer.writeBytes(nullMap);
      nullMap = null;
      for (byte[] row : rows) {
        if (row != null) {
          writer.writeLenencBytes(row);
        }
      }
      return writer.toByteArray();
    }
  }

  private static int generateBinaryRowHeader(byte[][] rows, byte[] nullMap) {
    int columnIndex = 0;
    int payloayEstimateMaxSize = 0;
    for (byte[] row : rows) {
      if (row != null) {
        payloayEstimateMaxSize += row.length;
        payloayEstimateMaxSize += MySQLPacket.getLenencLength(row.length);
      } else {
        int i = (columnIndex + 2) / 8;
        byte aByte = nullMap[i];
        nullMap[i] = (byte) (aByte | (1 << (columnIndex & 7)));
      }
      columnIndex++;
    }
    return payloayEstimateMaxSize;
  }

  public static final byte[] generateColumnDefPayload(String database, String table,
                                                      String originalTable,
                                                      String columnName, String orgName, int type,
                                                      int columnFlags,
                                                      int columnDecimals, int charsetIndex, int length, Charset charset) {
    ColumnDefPacketImpl c = new ColumnDefPacketImpl();
    c.setColumnSchema(database.getBytes(charset));
    c.setColumnOrgTable(originalTable.getBytes(charset));
    c.setColumnTable(table.getBytes(charset));
    c.setColumnCharsetSet(charsetIndex);
    c.setColumnLength(length);
    c.setColumnName(encode(columnName, charset));
    c.setColumnOrgName(encode(orgName, charset));
    c.setColumnType(type);
    c.setColumnFlags(columnFlags);
    c.setColumnDecimals((byte) columnDecimals);
    MySQLPayloadWriter writer = new MySQLPayloadWriter(64);
    c.writePayload(writer);
    return writer.toByteArray();
  }

  public static byte[] generateMySQLCommandRequest(int packetId, byte head, byte[] packet) {
    try (MySQLPayloadWriter byteArrayOutput = new MySQLPayloadWriter(1 + packet.length)) {
      byteArrayOutput.write(head);
      byteArrayOutput.write(packet);
      byte[] bytes = byteArrayOutput.toByteArray();
      return generateMySQLPacket(packetId, bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] generatePreparePayloadRequest(byte[] sql) {
    try (MySQLPayloadWriter byteArrayOutput = new MySQLPayloadWriter(1 + sql.length)) {
      byteArrayOutput.writeByte(0x16);
      byteArrayOutput.write(sql);
      byte[] bytes = byteArrayOutput.toByteArray();
      return bytes;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] generateMySQLPacket(int packetId, MySQLPayloadWriter writer) {
    byte[] bytes = writer.toByteArray();
    try {
      Thread thread = Thread.currentThread();
      PacketSplitterImpl  packetSplitter = new PacketSplitterImpl();
      int wholePacketSize = MySQLPacketSplitter.caculWholePacketSize(bytes.length);
      try (MySQLPayloadWriter byteArray = new MySQLPayloadWriter(
          wholePacketSize)) {
        packetSplitter.init(bytes.length);
        while (packetSplitter.nextPacketInPacketSplitter()) {
          int offset = packetSplitter.getOffsetInPacketSplitter();
          int len = packetSplitter.getPacketLenInPacketSplitter();
          byteArray.writeFixInt(3, len);
          byteArray.write(packetId);
          byteArray.write(bytes, offset, len);
          ++packetId;
        }
        return byteArray.toByteArray();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] generateMySQLPacket(int packetId, byte[] packet) {
    try {
      PacketSplitterImpl  packetSplitter = new PacketSplitterImpl();
      int wholePacketSize = MySQLPacketSplitter.caculWholePacketSize(packet.length);
      try (MySQLPayloadWriter byteArray = new MySQLPayloadWriter(
          wholePacketSize)) {
        packetSplitter.init(packet.length);
        while (packetSplitter.nextPacketInPacketSplitter()) {
          int offset = packetSplitter.getOffsetInPacketSplitter();
          int len = packetSplitter.getPacketLenInPacketSplitter();
          byteArray.writeFixInt(3, len);
          byteArray.write(packetId);
          byteArray.write(packet, offset, len);
          ++packetId;
        }
        return byteArray.toByteArray();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] encode(String src, String charset) {
    if (src == null) {
      return null;
    }
    try {
      return src.getBytes(charset);
    } catch (UnsupportedEncodingException e) {
      return src.getBytes();
    }
  }

  public static byte[] encode(String src, Charset charset) {
    if (src == null) {
      return new byte[]{};
    }
    return src.getBytes(charset);
  }


  /**
   * 计算字段值存放所需空间大小
   */
  public static int calcTextRowPayloadSize(byte[][] fieldValues) {
    int size = 0;
    int fieldCount = fieldValues.length;
    for (int i = 0; i < fieldCount; i++) {
      byte[] v = fieldValues[i];
      size += (v == null || v.length == 0) ? 1 : MySQLPacket.getLenencLength(v.length);
    }
    return size;
  }

  /**
   * @param fieldValues 字段值 数组为null就是字段值为null
   * @param writer 结果
   */
  public static void writeTextRow(byte[][] fieldValues, MySQLPayloadWriteView writer) {
    int fieldCount = fieldValues.length;
    for (int i = 0; i < fieldCount; i++) {
      byte[] fv = fieldValues[i];
      if (fv == null) {
        writer.writeByte(NULL_MARK);
      } else if (fv.length == 0) {
        writer.writeByte(EMPTY_MARK);
      } else {
        writer.writeLenencBytes(fv);
      }
    }
  }

  /**
   * @param fieldValues 字段值的数组
   */
  public static final byte[] generateTextRow(byte[][] fieldValues) {
    int len = calcTextRowPayloadSize(fieldValues);
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(len)) {
      writeTextRow(fieldValues, writer);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateLondData(long statementId, long paramId, byte[] data) {
    try (MySQLPayloadWriter out = new MySQLPayloadWriter(data.length + 8 + 8)) {
      out.write(0x18);
      out.writeFixInt(4, statementId);
      out.writeFixInt(2, paramId);
      out.write(data);
      return out.toByteArray();
    }
  }

  public static final byte[] generateChangeUser(
      String username,
      int serverCapabilities,
      String authenticationResponse,
      String defaultSchemaName,
      int clientCharSet,
      String authenticationPluginName,
      Map<String, String> attr
  ) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(512)) {
      writer.write(0x11);
      writer.writeNULString(username);
      if (MySQLServerCapabilityFlags.isCanDo41Anthentication(serverCapabilities)) {
        byte[] bytes = authenticationResponse.getBytes();
        writer.write(bytes.length);
        writer.write(bytes);
      } else {
        writer.writeNULString(authenticationResponse);
      }
      if (MySQLServerCapabilityFlags.isConnectionWithDatabase(serverCapabilities)) {
        writer.writeNULString(defaultSchemaName);
      }
      writer.writeFixInt(2, clientCharSet);
      if (MySQLServerCapabilityFlags.isPluginAuth(serverCapabilities)) {
        writer.writeNULString(authenticationPluginName);
      }
      if (MySQLServerCapabilityFlags.isConnectAttrs(serverCapabilities)) {
        if (attr != null && !attr.isEmpty()) {
          try (MySQLPayloadWriter mySQLPayloadWriter = new MySQLPayloadWriter(128)) {
            for (Entry<String, String> entry : attr.entrySet()) {
              String key = entry.getKey();
              String value = entry.getValue();
              mySQLPayloadWriter.writeLenencString(key);
              mySQLPayloadWriter.writeLenencString(value);
            }
            byte[] bytes = mySQLPayloadWriter.toByteArray();
            writer.writeLenencInt(bytes.length);
            writer.writeBytes(bytes);
          }
        }
      }
      return writer.toByteArray();
    }
  }

  public static final byte[] generateAuthenticationSwitchRequest(
      String authenticationPluginName,
      byte[] authenticationPluginData
  ) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(512)) {
      writer.writeFixInt(1, 0xfe);
      writer.writeNULString(authenticationPluginName);
      writer.write(authenticationPluginData);
      return writer.toByteArray();
    }
  }

  public static final byte[] generateAuthenticationSwitchResponse(
      byte[] authenticationResponseData) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(512)) {
      writer.writeEOFString(new String(authenticationResponseData));
      return writer.toByteArray();
    }
  }

  public static final byte[] generateSSLRequest(
      int clientCapacities,
      int maxPacketSize,
      int characterCollation,
      String reserved,
      int extendedClientCapabilitles,
      String reserved2
  ) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(512)) {
      writer.writeFixInt(4, clientCapacities);
      writer.writeFixInt(4, maxPacketSize);
      writer.writeFixInt(1, characterCollation);
      writer.write(reserved.getBytes());
      if (MySQLServerCapabilityFlags.isLongPassword(clientCapacities)) {
        writer.writeFixInt(4, extendedClientCapabilitles);
      } else {
        writer.write(reserved2.getBytes());
      }
      return writer.toByteArray();
    }
  }

  public static byte[] generateFetchPayload(long cursorStatementId, long numOfRows) {
    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(9)) {
      writer.writeByte(0x1c);
      writer.writeFixInt(4, cursorStatementId);
      writer.writeFixInt(4, numOfRows);
      return writer.toByteArray();
    }
  }
}
