package nl.anchormen.sql4es;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;

public class ESResultSetMetaData implements ResultSetMetaData{

	private List<Column> visibleCols;
	private String table;
	private String database;

	public ESResultSetMetaData(List<Column> visibleCols, String index, String type) {
		this.visibleCols = visibleCols;
		this.database = index;
		this.table = type;
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getColumnCount() throws SQLException {
		return visibleCols.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return ResultSetMetaData.columnNullable;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return Integer.MAX_VALUE;
	}

	@Override
	public String getColumnLabel(int idx) throws SQLException {
		idx--;
		if(idx < 0 || idx >= visibleCols.size()) throw new SQLException("Row does not contain column number wint index: "+idx);
		return visibleCols.get(idx).getLabel();
	}

	@Override
	public String getColumnName(int idx) throws SQLException {
		idx--;
		if(idx < 0 || idx >= visibleCols.size()) throw new SQLException("Row does not contain column number wint index: "+idx);
		return visibleCols.get(idx).getFullName();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return "";
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return table;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return database;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		column--;
		if(column < 0 || column >= visibleCols.size()) throw new SQLException("Row does not contain column number wint index: "+column);
		return visibleCols.get(column).getSqlType();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getColumnClassName(column);
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		column--;
		if(column < 0 || column >= visibleCols.size()) throw new SQLException("Row does not contain column number wint index: "+column);
		int type = visibleCols.get(column).getSqlType();
		return Heading.getClassForTypeId(type).getName();
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(Column col : visibleCols){
			sb.append(col.getIndex()+" : ");
			sb.append("Label: "+col.getLabel());
			sb.append("\tColumn: "+col.getColumn());
			sb.append("\tName: "+col.getFullName());
			sb.append("\tType: "+col.getSqlType());
			sb.append("\tClass: "+Heading.getClassForTypeId(col.getSqlType()));
			sb.append("\r\n");
		}
		return sb.toString();
	}

}
