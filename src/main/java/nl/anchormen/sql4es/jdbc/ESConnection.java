package nl.anchormen.sql4es.jdbc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.anchormen.sql4es.ESDatabaseMetaData;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.Utils;

/**
 * A {@link Connection} implementation effectively wrapping an Elasticsearch {@link Client}.
 * 
 * @author cversloot
 *
 */
public class ESConnection implements Connection{

	private String host;
	private int port;
	private String index;
	private Properties props;
	
	private Client client;
	private boolean active = true;
	private int timeout = Integer.MAX_VALUE;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private boolean autoCommit = false;
	private boolean readOnly = true;
	private List<ESStatement> statements = new ArrayList<ESStatement>();
	private int transactionIsolation;
	/**
	 * Builds the es {@link Client} using the provided parameters. 
	 * @param host
	 * @param port
	 * @param index
	 * @param props the properties will all be copied to the Settings.Builder used to create the Client
	 * @throws SQLException
	 */
	public ESConnection(String host, int port, String index, Properties props) throws SQLException{
		this.index = index;
		this.port = port;
		this.host = host;
		this.props = props;
		this.client = buildClient();
		try{
			this.getTypeMap(); // loads types into properties
		}catch(Exception e){
			throw new SQLException("Unable to connect to specified elasticsearch host(s)", e);
		}
	}
	
	/**
	 * Builds the Elasticsearch client using the properties this connection was 
	 * instantiated with
	 * @return
	 * @throws SQLException
	 */
	private Client buildClient() throws SQLException {
		/*if(props.containsKey("test")){ // used for integration tests
			return org.elasticsearch.test.ESIntegTestCase.client();
		}else*/ try {
			Settings.Builder settingsBuilder = Settings.builder();

			for(Object key : this.props.keySet()){
				// remove driver properties, since es 5.0 the client throws errors for unknown properties
				if(key.equals(Utils.PROP_DEFAULT_ROW_LENGTH) ||
					key.equals(Utils.PROP_FETCH_SIZE) ||
					key.equals(Utils.PROP_FRAGMENT_NUMBER) ||
					key.equals(Utils.PROP_FRAGMENT_SIZE) ||
					key.equals(Utils.PROP_QUERY_CACHE_TABLE) ||
					key.equals(Utils.PROP_QUERY_TIMEOUT_MS) ||
					key.equals(Utils.PROP_RESULT_NESTED_LATERAL) ||
					key.equals(Utils.PROP_SCROLL_TIMEOUT_SEC) ||
					key.equals(Utils.PROP_USER)||
					key.equals(Utils.PROP_PASSWORD)
				) continue;
				settingsBuilder.put(key, this.props.get(key));
			}
			if(props.containsKey("cluster.name")){
				settingsBuilder.put("request.headers.X-Found-Cluster", props.get("cluster.name"));
			}
			
			TransportClient client = null;
//			if(props.containsKey("xpack.security.user")){
//				client = new PreBuiltXPackTransportClient(settingsBuilder.build())
//						.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
//			}else{
				client = new PreBuiltTransportClient(settingsBuilder.build())
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
//			}
			// add additional hosts if set in URL query part
			if(this.props.containsKey("es.hosts"))
				for(String hostPort : this.props.getProperty("es.hosts").split(",")){
					String newHost = hostPort.split(":")[0].trim();
					int newPort = (hostPort.split(":").length > 1 ? Integer.parseInt(hostPort.split(":")[1]) : Utils.PORT);
					client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(newHost), newPort));
					logger.info("Adding additional ES host: "+hostPort);
			}
			
			// check if index exists
			if(index != null){
				boolean indexExists = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
				if(!indexExists) throw new SQLException("Index or Alias '"+index+"' does not exist");
			}
			return client;
		} catch (UnknownHostException e) {
			throw new SQLException ("Unable to connect to "+host, e);
		} catch (Throwable t){
			throw new SQLException("Unable to connect to database", t);
		}
	}
	
	public Client getClient(){
		return this.client;
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		//throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
		 //throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public Statement createStatement() throws SQLException {
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		return new ESStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		return new ESPreparedStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}
	
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		// TODO use params
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		return new ESStatement(this);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		 this.autoCommit  = autoCommit;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		//return autoCommit;
		return true;
	}

	@Override
	public void commit() throws SQLException {
//		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void rollback() throws SQLException {
//		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void close() throws SQLException {
		if(isClosed()) return;
		for(ESStatement st : this.statements) st.close();
		statements.clear();
		client.close();
		this.active = false;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return !active;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return new ESDatabaseMetaData(host, port, client, this.getClientInfo(), this);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {	
		 this.readOnly = readOnly;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return readOnly ;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		 //throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public String getCatalog() throws SQLException {
		 return null;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
//		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
//		transactionIsolation=level;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
//		return transactionIsolation;
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		 // TODO
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		// TODO use params?
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		return new ESPreparedStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		ResultSet rs = getMetaData().getColumns(null, null, null, null);
		Map<String, Map<String, Integer>> tableColumnInfo = new HashMap<String, Map<String, Integer>>();;
		while(rs.next()){
			String table = rs.getString(3);
			String col = rs.getString(4);
			int type = rs.getInt(5);
			if(!tableColumnInfo.containsKey(table)) tableColumnInfo.put(table, new HashMap<String, Integer>());
			tableColumnInfo.get(table).put(col, type);
		}
		this.props.put(Utils.PROP_TABLE_COLUMN_MAP, tableColumnInfo);
		
		Map<String, Class<?>> result = new HashMap<String, Class<?>>();
		for(String type : tableColumnInfo.keySet()){
			for(String field : tableColumnInfo.get(type).keySet()){
				result.put(type+"."+field, Heading.getClassForTypeId(tableColumnInfo.get(type).get(field)));
			}
		}
		return result;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public int getHoldability() throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		 throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		if(isClosed()) throw new SQLException("Connection closed");
		ESStatement st = new ESStatement(this);
		statements.add(st);
		return st;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		// TODO use params
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		if(isClosed()) throw new SQLException("Connection closed");
		
		ESPreparedStatement st = new ESPreparedStatement(this, sql);
		statements.add(st);
		return st;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		// TODO use params
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		if(isClosed()) throw new SQLException("Connection closed");
		
		ESPreparedStatement st = new ESPreparedStatement(this, sql);
		statements.add(st);
		return st;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		// TODO use params
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		if(isClosed()) throw new SQLException("Connection closed");
		
		ESPreparedStatement st = new ESPreparedStatement(this, sql);
		statements.add(st);
		return st;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		// TODO use params
		if(this.client == null){
			throw new SQLException("Unable to connect on specified schema '"+this.index+"'");
		}
		if(isClosed()) throw new SQLException("Connection closed");
		
		ESPreparedStatement st = new ESPreparedStatement(this, sql);
		statements.add(st);
		return st;
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return active;
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		props.setProperty(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		this.props = properties;
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return props.getProperty(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return this.props;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return null;
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		boolean indexExists = client.admin().indices().exists(new IndicesExistsRequest(schema)).actionGet().isExists();
		if(!indexExists) throw new SQLException("Index '"+schema+"' does not exist");
		this.index = schema;
	}

	@Override
	public String getSchema() throws SQLException {
		return this.index;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		this.timeout = milliseconds;
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return timeout;
	}

}
