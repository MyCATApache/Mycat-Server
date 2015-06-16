package org.opencloudb.jdbc.sequoiadb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
/**  
 * 功能详细描述
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2014年12月19日 下午6:50:23 
 * @version 0.0.1
 */
public class SequoiaPreparedStatement extends SequoiaStatement implements
		PreparedStatement {
	final String _sql;
	final SequoiaSQLParser _mongosql;
	List _params = new ArrayList();

	public SequoiaPreparedStatement(SequoiaConnection conn, int type,
			int concurrency, int holdability, String sql)
			throws SequoiaSQLException {
		super(conn, type, concurrency, holdability);
		this._sql = sql;
		this._mongosql = new SequoiaSQLParser(conn.getDB(), sql);
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		
		return null;
	}

	@Override
	public int executeUpdate() throws SQLException {
		
	    this._mongosql.setParams(this._params);
	    return this._mongosql.executeUpdate();
	}
	
	public  void setValue(int idx, Object o) {	  
	    while (this._params.size() <= idx)
	      this._params.add(null);
	    this._params.set(idx, o);
	 }
	  
	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		

	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		
		setValue(parameterIndex, Boolean.valueOf(x));
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		
		setValue(parameterIndex, Byte.valueOf(x));
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		
		setValue(parameterIndex, Short.valueOf(x));
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		
		setValue(parameterIndex, Integer.valueOf(x));
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		
		setValue(parameterIndex, Long.valueOf(x));
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		
		setValue(parameterIndex, Float.valueOf(x));
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		
		setValue(parameterIndex, Double.valueOf(x));
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		
		setValue(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		
		setValue(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		
		setValue(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		
		setValue(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		
		setValue(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		
		setValue(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		

	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		

	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		

	}

	@Override
	public void clearParameters() throws SQLException {
		

	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		

	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		
		setValue(parameterIndex,x);
	}

	@Override
	public boolean execute() throws SQLException {
		
		return false;
	}

	@Override
	public void addBatch() throws SQLException {
		

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		

	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		

	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		

	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		

	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		

	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		
		return null;
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		

	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		

	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		

	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		

	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		

	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		
		return null;
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		

	}

	@Override
	public void setNString(int parameterIndex, String value)
			throws SQLException {
		

	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		

	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		

	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		

	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		

	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		

	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		

	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		

	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		

	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		

	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		

	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		

	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		

	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		

	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		

	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		

	}

}
