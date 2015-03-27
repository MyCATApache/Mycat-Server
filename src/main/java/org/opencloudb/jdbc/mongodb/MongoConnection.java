package org.opencloudb.jdbc.mongodb;

import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
/**  
 * 功能详细描述
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2014年12月19日 下午6:50:23 
 * @version 0.0.1
 */

public class MongoConnection implements Connection {
	
	//private String url = null;
	private MongoClient mc = null;	
	private boolean isClosed = false;	
	private String _schema;
	private Properties _clientInfo;

	public MongoConnection(MongoClientURI mcu, String url) throws UnknownHostException {
	//	this.url = url;
		this._schema = mcu.getDatabase();		
		mc = new MongoClient(mcu);
	}
	
	public DB getDB()  {
		if (this._schema!=null) {
	      return this.mc.getDB(this._schema);
		}
		else return null;
	}
	   
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		// TODO Auto-generated method stub
	    if (!autoCommit)  
		  throw new RuntimeException("autoCommit has to be on");	
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		// TODO Auto-generated method stub
		return true;//return false;
	}

	@Override
	public void commit() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback() throws SQLException {
		// TODO Auto-generated method stub
		//throw new RuntimeException("can't rollback");
	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		this.mc=null;
	    isClosed=true;	
	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return isClosed;//return false;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		// 获取一个 DatabaseMetaData 对象，该对象包含关于此 Connection 对象所连接的数据库的元数据。
		return null;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		// TODO Auto-generated method stub
		//if (readOnly)
		//    throw new RuntimeException("no read only mode");		
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// 查询此 Connection 对象是否处于只读模式。
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		// TODO Auto-generated method stub
		this._schema=catalog;
	}

	@Override
	public String getCatalog() throws SQLException {
		// 获取此 Connection 对象的当前目录名称
		return this._schema;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		// TODO Auto-generated method stub
	   //throw new RuntimeException("no TransactionIsolation");
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;//throw new RuntimeException("should do get last error");
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
	   
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		// 将使用此 Connection 对象创建的 ResultSet 对象的默认可保存性 (holdability) 更改为给定可保存性。
		
	}

	@Override
	public int getHoldability() throws SQLException {
		// 获取使用此 Connection 对象创建的 ResultSet 对象的当前可保存性。
		return 0;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return null;//throw new RuntimeException("no savepoints");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub
		// throw new RuntimeException("can't rollback");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Statement createStatement() throws SQLException {
		// 创建一个 Statement 对象来将 SQL 语句发送到数据库。
		return createStatement(0, 0, 0);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		// 创建一个 Statement 对象，该对象将生成具有给定类型和并发性的 ResultSet 对象。
		return createStatement(resultSetType, resultSetConcurrency, 0);
	}	
	
	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// 创建一个 Statement 对象，该对象将生成具有给定类型、并发性和可保存性的 ResultSet 对象。
		return new MongoStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	
	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return prepareCall(sql, 0, 0, 0);
	}
	
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return prepareCall(sql, resultSetType, resultSetConcurrency, 0);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		//return null;
		throw new RuntimeException("CallableStatement not supported");
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return prepareStatement(sql, 0, 0, 0);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return prepareStatement(sql, resultSetType, resultSetConcurrency, 0);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return new MongoPreparedStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability,sql);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		// TODO Auto-generated method stub
		return this.mc.getDB(_schema) != null;
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
		this._clientInfo.put(name, value);
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
		this._clientInfo = properties;
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		// 返回通过名称指定的客户端信息属性的值。
		return (String)this._clientInfo.get(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// 返回一个列表，它包含驱动程序支持的每个客户端信息属性的名称和当前值。
		return this._clientInfo;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		// TODO Auto-generated method stub
		//this._schema=schema;
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return this._schema;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

 }

