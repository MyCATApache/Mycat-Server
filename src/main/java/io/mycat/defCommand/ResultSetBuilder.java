package io.mycat.defCommand;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ResultSetBuilder {

    final List<ColumnInfo> columnInfos = new ArrayList<>();
    final List<Object[]> objectList = new ArrayList<>();

    public static ResultSetBuilder create() {
        return new ResultSetBuilder();
    }

    public ResultSetBuilder() {
        columnInfos.add(null);
    }

    public void addColumnInfo(String schemaName, String tableName, String columnName, int columnType, int precision, int scale, String columnLabel, boolean isAutoIncrement, boolean isCaseSensitive, boolean isNullable, boolean isSigned, int displaySize) {
        columnInfos.add(new ColumnInfo(schemaName, tableName, columnName, columnType, precision, scale, columnLabel, isAutoIncrement, isCaseSensitive, isNullable, isSigned, displaySize));
    }

    public void addColumnInfo(String tableName, String columnName, int columnType, int precision, int scale) {
        columnInfos.add(new ColumnInfo(tableName, tableName, columnName, columnType, precision, scale, columnName, false, true, true, true, columnName.length()));
    }

    public void addColumnInfo(String columnName, JDBCType columnType) {
        addColumnInfo(columnName, columnType.getVendorTypeNumber());
    }

    public void addColumnInfo(String columnName, int columnType) {
        columnInfos.add(new ColumnInfo(columnName, columnType));
    }


    public int columnCount() {
        return columnInfos.size();
    }


    public void addObjectRowPayload(Object row) {
        objectList.add(new Object[]{row});
    }

    public void addObjectRowPayload(List row) {
        objectList.add(row.toArray());
    }

    public RowBaseIterator build() {
        DefMycatRowMetaData mycatRowMetaData = new DefMycatRowMetaData(columnInfos);
        int columnCount = mycatRowMetaData.getColumnCount();
        return new DefObjectRowIteratorImpl(mycatRowMetaData, objectList.iterator());
    }

    /**
     * @author Junwen Chen
     **/
    private static class DefMycatRowMetaData implements MycatRowMetaData {
        final List<ColumnInfo> columnInfos;

        public DefMycatRowMetaData(List<ColumnInfo> columnInfos) {
            this.columnInfos = columnInfos;
        }

        @Override
        public int getColumnCount() {
            return columnInfos.size()-1;
        }

        @Override
        public boolean isAutoIncrement(int column) {
            return columnInfos.get(column).isAutoIncrement();
        }

        @Override
        public boolean isCaseSensitive(int column) {
            return columnInfos.get(column).isCaseSensitive();
        }

        @Override
        public boolean isNullable(int column) {
            return columnInfos.get(column).isNullable();
        }

        @Override
        public boolean isSigned(int column) {
            return columnInfos.get(column).isSigned();
        }

        @Override
        public int getColumnDisplaySize(int column) {
            return columnInfos.get(column).getDisplaySize();
        }

        @Override
        public String getColumnName(int column) {
            return columnInfos.get(column).getColumnName();
        }

        @Override
        public String getSchemaName(int column) {
            return columnInfos.get(column).getSchemaName();
        }

        @Override
        public int getPrecision(int column) {
            return columnInfos.get(column).getPrecision();
        }

        @Override
        public int getScale(int column) {
            return columnInfos.get(column).getScale();
        }

        @Override
        public String getTableName(int column) {
            return columnInfos.get(column).getTableName();
        }

        @Override
        public int getColumnType(int column) {
            return columnInfos.get(column).getColumnType();
        }

        @Override
        public String getColumnLabel(int column) {
            return columnInfos.get(column).getColumnLabel();
        }

        @Override
        public ResultSetMetaData metaData() {
            throw new UnsupportedOperationException();
        }
    }



    static public class DefObjectRowIteratorImpl extends AbstractObjectRowIterator {
        final DefMycatRowMetaData mycatRowMetaData;
        final Iterator<Object[]> iterator;
        boolean close = false;

        public DefObjectRowIteratorImpl(DefMycatRowMetaData mycatRowMetaData, Iterator<Object[]> iterator) {
            this.mycatRowMetaData = mycatRowMetaData;
            this.iterator = iterator;
        }

        @Override
        public MycatRowMetaData getMetaData() {
            return mycatRowMetaData;
        }

        @Override
        public boolean next() {
            if (this.iterator.hasNext()) {
                this.currentRow = this.iterator.next();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void close() {
            close = true;
        }

    }

    /**
     * 跳过头部的null
     * @return
     */
    public List<ColumnInfo> getColumnInfos() {
        return columnInfos.subList(1,columnInfos.size());
    }
}