package nl.anchormen.sql4es.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Use;

import nl.anchormen.sql4es.ESQueryState;
import nl.anchormen.sql4es.ESResultSet;
import nl.anchormen.sql4es.ESUpdateState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.Utils;

public class ESStatement implements Statement {

	private static final SqlParser parser = new SqlParser();
	protected ESConnection connection;
	
	protected int queryTimeoutSec = 10;
	protected boolean poolable = true;
	protected boolean closeOnCompletion = false;
	protected ResultSet result;

	protected ESQueryState queryState;
	protected ESUpdateState updateState;
	
	public ESStatement(ESConnection connection) throws SQLException{
		this.connection = connection;
		this.queryState = new ESQueryState(connection.getClient(), this);
		updateState = new ESUpdateState(connection.getClient(), this);
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
	public ResultSet executeQuery(String sql) throws SQLException {
		//System.out.println("QUERY: ["+sql+"]");
		if(connection.getSchema() == null) throw new SQLException("No active index set for this driver. Pleas specify an active index or alias by executing 'USE <index/alias>' first");
		sql = sql.replaceAll("\r", " ").replaceAll("\n", " ");
		com.facebook.presto.sql.tree.Statement statement = parser.createStatement(sql);
		if(statement instanceof Query){
			if(this.result != null) this.result.close();
			queryState.buildRequest(sql, ((Query)statement).getQueryBody(), connection.getSchema());
			this.result = queryState.execute();
			return this.result;
		}else if(statement instanceof Explain){
			String ex = queryState.explain(sql, (Explain)statement, connection.getSchema());
			if(this.result != null) this.result.close();
			Heading heading = new Heading();
			heading.add(new Column("Explanation"));
			ESResultSet rs = new ESResultSet(heading, 1, 1);
			List<Object> row = rs.getNewRow();
			row.set(0, ex);
			rs.add(row);
			this.result = rs;
			return result;
		}else throw new SQLException("Provided query is not a SELECT or EXPLAIN query");
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		//System.out.println("QUERY: ["+sql+"]");
		sql = sql.replaceAll("\r", " ").replaceAll("\n", " ").trim();
		// custom stuff to support UPDATE statements since Presto does not parse it
		if(sql.toLowerCase().startsWith("update")){
			return updateState.execute(sql);
		}
		
		com.facebook.presto.sql.tree.Statement statement = parser.createStatement(sql);
		if(statement instanceof Query) throw new SQLException("A regular query cannot be executed as an Update");
		if(statement instanceof Insert){
			//if(connection.getSchema() == null) throw new SQLException("No active index set for this driver. Pleas specify an active index or alias by executing 'USE <index/alias>' first");
			return updateState.execute(sql, (Insert)statement, connection.getSchema());
		}else if(statement instanceof Delete){
			if(connection.getSchema() == null) throw new SQLException("No active index set for this driver. Pleas specify an active index or alias by executing 'USE <index/alias>' first");
			return updateState.execute(sql, (Delete)statement, connection.getSchema());
		}else if(statement instanceof CreateTable){
			return updateState.execute(sql, (CreateTable)statement, connection.getSchema());
		}else if(statement instanceof CreateTableAsSelect){
			return updateState.execute(sql, (CreateTableAsSelect)statement, connection.getSchema());
		}else if(statement instanceof CreateView){
			return updateState.execute(sql, (CreateView)statement, connection.getSchema());
		}else if(statement instanceof Use){
			connection.setSchema( ((Use)statement).getSchema());
			connection.getTypeMap(); // updates the type mappings found in properties
			return 0;
		}else if(statement instanceof DropTable){
			return updateState.execute(sql, (DropTable)statement);
		}else if(statement instanceof DropView){
			return updateState.execute(sql, (DropView)statement);
		}throw new SQLFeatureNotSupportedException("Unable to parse provided update sql");
	}

	@Override
	public void close() throws SQLException {
		queryState.close();
		updateState.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxRows() throws SQLException {
		return this.queryState.getMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		this.queryState.setMaxRows(max);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return queryTimeoutSec;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		this.queryTimeoutSec = seconds;
	}

	@Override
	public void cancel() throws SQLException {
		// TODO Auto-generated method stub
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
	public void setCursorName(String name) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		//System.out.println("QUERY: ["+sql+"]");
		sql = sql.replaceAll("\r", " ").replaceAll("\n", " ");
		String sqlNorm = sql.trim().toLowerCase();
		if(sqlNorm.startsWith("select") || sqlNorm.startsWith("explain")) {
			this.result = this.executeQuery(sql);
			return result != null;
		}else if(sqlNorm.startsWith("insert") || sqlNorm.startsWith("delete")
				|| sqlNorm.startsWith("create") || sqlNorm.startsWith("use") ||
				sqlNorm.startsWith("drop")||sqlNorm.startsWith("update")) {
			this.executeUpdate(sql);
			return false;
		}else throw new SQLException("Provided query type '"+sql.substring(0, sql.indexOf(' '))+"' is not supported");
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return this.result;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return -1;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		this.result = queryState.moreResutls(Utils.getBooleanProp(this.connection.getClientInfo(), Utils.PROP_RESULT_NESTED_LATERAL, true));
		return result != null;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		// Not able to set this one
	}

	@Override
	public int getFetchSize() throws SQLException {
		return Utils.getIntProp(getConnection().getClientInfo(), Utils.PROP_FETCH_SIZE, 10000);
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		sql = sql.trim().replaceAll("\r", " ").replaceAll("\n", " ");
		updateState.addToBulk(sql, this.getConnection().getSchema());
	}

	@Override
	public void clearBatch() throws SQLException {
		this.updateState.clearBulk();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return this.updateState.executeBulk();
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		// TODO use current param
		ResultSet newResult = queryState.moreResutls(Utils.getBooleanProp(this.connection.getClientInfo(), Utils.PROP_RESULT_NESTED_LATERAL, true));
		if(newResult == null) return false;
		this.result = newResult;
		return true;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return this.executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return this.executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return this.executeUpdate(sql);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return this.execute(sql);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return this.execute(sql);
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return this.execute(sql);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		this.poolable = poolable;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return poolable;
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		this.closeOnCompletion = true;
		
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return closeOnCompletion;
	}

}
