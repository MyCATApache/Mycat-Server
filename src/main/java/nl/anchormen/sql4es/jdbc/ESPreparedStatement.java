package nl.anchormen.sql4es.jdbc;

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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import nl.anchormen.sql4es.ESParameterMetaData;
import nl.anchormen.sql4es.model.Utils;

public class ESPreparedStatement extends ESStatement implements PreparedStatement{

	private Object[] sqlAndParams;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	
	public ESPreparedStatement(ESConnection connection, String sql) throws SQLException{
		super(connection);
		sql = sql.trim();

		String[] parts = (sql+";").split("\\?");
		this.sqlAndParams = new Object[parts.length*2-1];
		for(int i=0; i<parts.length; i++){
			this.sqlAndParams[i*2] = parts[i];
		}
	}
	
	/**
	 * Builds the final sql statement
	 * @return
	 * @throws SQLException
	 */
	private String buildSql() throws SQLException{
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<sqlAndParams.length; i++)try {
			if(sqlAndParams[i] instanceof Date){
				sb.append("'"+dateFormat.format((Date)sqlAndParams[i])+"' ");
			}else{
				sb.append(sqlAndParams[i]+" ");
			}
		}catch(Exception e){
			throw new SQLException("Unable to create SQL statement, ["+i+"] = "+sqlAndParams[i]+" : "+e.getMessage(), e);
		}
		return sb.substring(0, sb.length()-2);
	}
	
	@Override
	public ResultSet executeQuery() throws SQLException {
		if(super.execute(this.buildSql())) return getResultSet();
		else return null;
	}

	@Override
	public int executeUpdate() throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = null;
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Boolean(x);
		
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Byte(x);
		
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Short(x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Integer(x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Long(x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Float(x);		
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Double(x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = x;
		
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		// TODO: escape
		this.sqlAndParams[(parameterIndex*2) - 1] = "'"+x+"'";
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = x;	
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = x;
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Long(x.getTime());
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = new Long(x.getTime());
		
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());		
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());		
	}

	@Override
	public void clearParameters() throws SQLException {
		for(int i=1; i<sqlAndParams.length; i+=2) sqlAndParams[i] = null;
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		if(x instanceof String) this.sqlAndParams[(parameterIndex*2) - 1] = "'"+x+"'";
		else this.sqlAndParams[(parameterIndex*2) - 1] = x;
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if(x instanceof String ) this.sqlAndParams[(parameterIndex*2) - 1] = "'"+x+"'";
		else this.sqlAndParams[(parameterIndex*2) - 1] = x;
	}

	@Override
	public boolean execute() throws SQLException {
		return super.execute(this.buildSql());
	}

	@Override
	public void addBatch() throws SQLException {
		super.addBatch(this.buildSql());
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());		
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = x;	
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return result.getMetaData();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = null;
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = x;
		
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return new ESParameterMetaData(sqlAndParams);
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		this.sqlAndParams[(parameterIndex*2) - 1] = value;
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}


	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}
}
