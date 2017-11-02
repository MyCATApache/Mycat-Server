package nl.anchormen.sql4es.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import nl.anchormen.sql4es.QueryState;

/**
 * An implementation of {@link QueryState} used to hold information created during SQL parsing by the Presto parser.
 * Most importantly it hods the {@link Heading} containing the fields to be requested which is used throughout the 
 * query execution and parsing of results.
 * 
 * @author cversloot
 *
 */
public class BasicQueryState implements QueryState {

	private Heading heading;
	private Properties props;
	private String sql;
	private SQLException exception = null;
	private HashMap<String, Object> kvStore = new HashMap<String, Object>();
	private List<QuerySource> relations = new ArrayList<QuerySource>();

	public BasicQueryState(String sql, Heading heading, Properties props){
		this.heading = heading;
		this.props = props;
		this.sql = sql;
	}
	
	@Override
	public String originalSql() {
		return sql;
	}

	@Override
	public Heading getHeading() {
		return heading;
	}

	@Override
	public List<QuerySource> getSources(){
		return this.relations ;
	}
	
	public void setRelations(List<QuerySource> relations){
		this.relations = relations;
	}
	
	@Override
	public void addException(String msg) {
		this.exception = new SQLException(msg);		
	}

	@Override
	public boolean hasException() {
		return this.exception != null;
	}

	@Override
	public SQLException getException() {
		return this.exception;
	}

	@Override
	public int getIntProp(String name, int def) {
		return Utils.getIntProp(props, name, def);
	}

	@Override
	public String getProperty(String name, String def) {
		if(!this.props.containsKey(name)) return def;
		try {
			return this.props.getProperty(name);
		} catch (Exception e) {
			return def;
		}
	}

	@Override
	public Object getProperty(String name) {
		return this.props.get(name);
	}

	@Override
	public QueryState setKeyValue(String key, Object value) {
		kvStore.put(key, value);
		return this;
	}

	@Override
	public Object getValue(String key) {
		return kvStore.get(key);
	}

	@Override
	public String getStringValue(String key) {
		return (String)kvStore.get(key);
	}

}
