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
import io.mycat.util.StringUtil;

import java.sql.ResultSetMetaData;
import java.util.Arrays;

/**
 * @author jamie12221 date 2019-05-07 13:58
 *
 * 字段包实现
 **/
public class ColumnDefPacketImpl implements ColumnDefPacket {

    byte[] columnCatalog;
    byte[] columnSchema;
    byte[] columnTable;
    byte[] columnOrgTable;
    byte[] columnName;
    byte[] columnOrgName;
    int columnNextLength = 0xC;
    int columnCharsetSet;
    int columnLength = 256;
    int columnType;
    int columnFlags;
    byte columnDecimals;
    byte[] columnDefaultValues;

    final static byte[] EMPTY = new byte[]{};

    public ColumnDefPacketImpl() {
    }

    public ColumnDefPacketImpl(final ResultSetMetaData resultSetMetaData, int columnIndex) {
        try {
            this.columnSchema = resultSetMetaData.getSchemaName(columnIndex).getBytes();
            this.columnName = resultSetMetaData.getColumnLabel(columnIndex).getBytes();
            this.columnOrgName = resultSetMetaData.getColumnName(columnIndex).getBytes();
            this.columnNextLength = 0xC;
            this.columnLength = resultSetMetaData.getColumnDisplaySize(columnIndex);
            this.columnType = MySQLFieldsType.fromJdbcType(resultSetMetaData.getColumnType(columnIndex));
            this.columnDecimals = (byte) resultSetMetaData.getScale(columnIndex);
            this.columnCharsetSet = 0x21;
        } catch (Exception e) {
            throw new MycatException(e);
        }
    }

    byte[] getBytes(String text){
        if(text==null||"".equals(text)){
            return EMPTY;
        }
        return text.getBytes();
    }
    public ColumnDefPacketImpl(final MycatRowMetaData  resultSetMetaData, int columnIndex) {
        try {
            String schemaName = resultSetMetaData.getSchemaName(columnIndex);
            if (StringUtil.isEmpty(schemaName )){
                schemaName = "UNKNOWN";//mysql workbench 该字段不能为长度0
            }
            this.columnSchema = getBytes(schemaName);
            this.columnName = getBytes(resultSetMetaData.getColumnLabel(columnIndex));
            this.columnOrgName = getBytes(resultSetMetaData.getColumnName(columnIndex));
            this.columnNextLength = 0xC;
            this.columnLength = resultSetMetaData.getColumnDisplaySize(columnIndex);
            this.columnType = MySQLFieldsType.fromJdbcType(resultSetMetaData.getColumnType(columnIndex));
            this.columnDecimals = (byte) resultSetMetaData.getScale(columnIndex);
            this.columnCharsetSet = 0x21;
        } catch (Exception e) {
            throw new MycatException(e);
        }
    }


    public ColumnDefPacket toColumnDefPacket(MySQLFieldInfo def, String alias) {
        ColumnDefPacket columnDefPacket = new ColumnDefPacketImpl();
        columnDefPacket.setColumnCatalog(columnDefPacket.DEFAULT_CATALOG);
        columnDefPacket.setColumnSchema(def.getSchemaName().getBytes());
        columnDefPacket.setColumnTable(def.getTableName().getBytes());
        if (alias == null) {
            alias = def.getName();
        }
        columnDefPacket.setColumnName(alias.getBytes());
        columnDefPacket.setColumnOrgName(def.getName().getBytes());
        columnDefPacket.setColumnNextLength(0xC);
        columnDefPacket.setColumnCharsetSet(def.getCollationId());
        columnDefPacket.setColumnLength(def.getColumnMaxLength());
        columnDefPacket.setColumnType(def.getFieldType());
        columnDefPacket.setColumnFlags(def.getFieldDetailFlag());
        columnDefPacket.setColumnDecimals(def.getDecimals());
        columnDefPacket.setColumnDefaultValues(def.getDefaultValues());
        return columnDefPacket;
    }

    public void writePayload(MySQLPayloadWriteView buffer) {
        buffer.writeLenencBytesWithNullable(ColumnDefPacket.DEFAULT_CATALOG);
        buffer.writeLenencBytesWithNullable(columnSchema);
        buffer.writeLenencBytesWithNullable(columnTable);
        buffer.writeLenencBytesWithNullable(columnOrgTable);
        buffer.writeLenencBytesWithNullable(columnName);
        buffer.writeLenencBytesWithNullable(columnOrgName);
        buffer.writeLenencInt(0x0c);
        buffer.writeFixInt(2, columnCharsetSet);
        buffer.writeFixInt(4, columnLength);
        buffer.writeByte(columnType);
        buffer.writeFixInt(2, columnFlags);
        buffer.writeByte(columnDecimals);
        buffer.writeByte((byte) 0x00);//filler
        buffer.writeByte((byte) 0x00);//filler
        if (columnDefaultValues != null) {
            buffer.writeLenencString(columnDefaultValues);
        }
    }

    @Override
    public String toString() {
        return "ColumnDefPacketImpl{" +
                "columnCatalog=" + new String(ColumnDefPacket.DEFAULT_CATALOG) +
                ", columnSchema=" + new String(columnSchema) +
                ", columnTable=" + new String(columnTable) +
                ", columnOrgTable=" + new String(columnOrgTable) +
                ", columnName=" + new String(columnName) +
                ", columnOrgName=" + new String(columnOrgName) +
                ", columnNextLength=" + columnNextLength +
                ", columnCharsetSet=" + columnCharsetSet +
                ", columnLength=" + columnLength +
                ", columnType=" + columnType +
                ", columnFlags=" + columnFlags +
                ", columnDecimals=" + columnDecimals +
                ", columnDefaultValues=" + Arrays.toString(columnDefaultValues) +
                '}';
    }


    @Override
    public byte[] getColumnCatalog() {
        return ColumnDefPacket.DEFAULT_CATALOG;
    }

    @Override
    public void setColumnCatalog(byte[] catalog) {
        this.columnCatalog = catalog;
    }

    @Override
    public byte[] getColumnSchema() {
        return columnSchema;
    }

    @Override
    public void setColumnSchema(byte[] schema) {
        this.columnSchema = schema;
    }

    @Override
    public byte[] getColumnTable() {
        return columnTable;
    }

    @Override
    public void setColumnTable(byte[] table) {
        this.columnTable = table;
    }

    @Override
    public byte[] getColumnOrgTable() {
        return columnOrgTable;
    }

    @Override
    public void setColumnOrgTable(byte[] orgTable) {
        this.columnOrgTable = orgTable;
    }

    @Override
    public byte[] getColumnName() {
        return columnName;
    }

    @Override
    public void setColumnName(byte[] name) {
        this.columnName = name;
    }

    @Override
    public byte[] getColumnOrgName() {
        return columnOrgName;
    }

    @Override
    public void setColumnOrgName(byte[] orgName) {
        this.columnOrgName = orgName;
    }

    @Override
    public int getColumnNextLength() {
        return columnNextLength;
    }

    @Override
    public void setColumnNextLength(int nextLength) {
        this.columnLength = nextLength;
    }

    @Override
    public int getColumnCharsetSet() {
        return columnCharsetSet;
    }

    @Override
    public void setColumnCharsetSet(int charsetSet) {
        this.columnCharsetSet = charsetSet;
    }

    @Override
    public int getColumnLength() {
        return columnLength;
    }

    @Override
    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    @Override
    public int getColumnType() {
        return columnType;
    }

    @Override
    public void setColumnType(int type) {
        this.columnType = type;
    }

    @Override
    public int getColumnFlags() {
        return columnFlags;
    }

    @Override
    public void setColumnFlags(int flags) {
        this.columnFlags = flags;
    }

    @Override
    public byte getColumnDecimals() {
        return this.columnDecimals;
    }

    @Override
    public void setColumnDecimals(byte decimals) {
        this.columnDecimals = decimals;
    }

    @Override
    public byte[] getColumnDefaultValues() {
        return columnDefaultValues;
    }

    @Override
    public void setColumnDefaultValues(byte[] defaultValues) {
        this.columnDefaultValues = defaultValues;
    }


}
