package org.opencloudb.jdbc.sequoiadb;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**  
 * 功能详细描述
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2014年12月19日 下午6:50:23 
 * @version 0.0.1
 */

public class SequoiaResultSetMetaData implements ResultSetMetaData {
	
	private String[] keySet ;
	private int[] keytype ;
	private String _schema;
	private String _table;
	
	/*
	public MongoResultSetMetaData(Set<String> keySet,String schema) {
		super();
		this.keySet = new String[keySet.size()];
		this.keySet = keySet.toArray(this.keySet);
		this._schema = schema;
	}
    */
	public SequoiaResultSetMetaData(String[] select,int [] ftype,String schema,String table) {
		super();
		this.keySet = select;
		this.keytype=ftype;
		this._schema = schema;
		this._table  =table;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		
		return false;
	}

	@Override
	public int getColumnCount() throws SQLException {
		if (keySet==null) return 0;
		else
		  return keySet.length;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		// 是否为自动编号的字段
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		//指示列的大小写是否有关系
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		//指示是否可以在 where 子句中使用指定的列
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		// 指示指定的列是否是一个哈希代码值
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		// 指示指定列中的值是否可以为 null。
		return 0;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		// 指示指定列中的值是否带正负号
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		
		return 50;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return keySet[column-1];
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return keySet[column-1];
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		
		return this._schema;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		//获取指定列的指定列宽
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		// 检索指定参数的小数点右边的位数。
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		
		return this._table;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		
		return this._schema;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		// 字段类型
		return keytype[column-1];//Types.VARCHAR;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		// 数据库特定的类型名称
		switch (keytype[column-1]){
		case  Types.INTEGER: return "INTEGER";
		case  Types.BOOLEAN:  return "BOOLEAN";
		case  Types.BIT: return "BITT"; 
		case  Types.FLOAT: return "FLOAT";
		case  Types.BIGINT: return "BIGINT";
		case  Types.DOUBLE:  return "DOUBLE";
		case  Types.DATE: return "DATE"; 
		case  Types.TIME: return "TIME";
		case  Types.TIMESTAMP: return "TIMESTAMP";
		default: return "varchar";
	   }
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		//指示指定的列是否明确不可写入
		return false;
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
		// 如果调用方法 ResultSet.getObject 从列中获取值，则返回构造其实例的 Java 类的完全限定名称
		return "Object";
	}

}
