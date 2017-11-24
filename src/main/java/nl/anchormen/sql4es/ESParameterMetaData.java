package nl.anchormen.sql4es;

import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import nl.anchormen.sql4es.model.Heading;

public class ESParameterMetaData implements ParameterMetaData{

	private Object[] sqlAndParams;
	
	public ESParameterMetaData(Object[] sqlAndParams) {
		this.sqlAndParams = sqlAndParams;
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
	public int getParameterCount() throws SQLException {
		return sqlAndParams.length/2;
	}

	@Override
	public int isNullable(int param) throws SQLException {
		return ParameterMetaData.parameterNullable;
	}

	@Override
	public boolean isSigned(int param) throws SQLException {
		return false;
	}

	@Override
	public int getPrecision(int param) throws SQLException {
		return 0;
	}

	@Override
	public int getScale(int param) throws SQLException {
		return 0;
	}

	@Override
	public int getParameterType(int param) throws SQLException {
		Object value = sqlAndParams[(param*2) - 1];
		if(value == null) return Types.OTHER;
		return Heading.getTypeIdForObject(value);
	}

	@Override
	public String getParameterTypeName(int param) throws SQLException {
		Object value = sqlAndParams[(param*2) - 1];
		if(value == null) return null;
		if(value instanceof String) return "VARCHAR";
		else if(value instanceof Integer) return "INTEGER";
		else if(value instanceof Long) return "BIGINT";
		else if(value instanceof Float) return "FLOAT";
		else if(value instanceof Double) return "DOUBLE";
		else if(value instanceof Boolean) return "BOOLEAN";
		else if(value instanceof Time) return "TIME";
		else if(value instanceof Timestamp) return "TIMESTAMP";
		else if(value instanceof Date) return "DATE";
		else return "UNKNOWN";
	}

	@Override
	public String getParameterClassName(int param) throws SQLException {
		Object value = sqlAndParams[(param*2) - 1];
		if(value == null) return null;
		return value.getClass().getName();
	}

	@Override
	public int getParameterMode(int param) throws SQLException {
		return ParameterMetaData.parameterModeUnknown;
	}

}
