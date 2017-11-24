package nl.anchormen.sql4es.model;

import com.facebook.presto.sql.tree.QueryBody;

public class QuerySource {

	private String source;
	private String alias;
	private QueryBody query;
	
	public QuerySource(String source) {
		this.source = source;
	}
	
	public QuerySource(String source, QueryBody query) {
		this.source = source;
		this.query = query;
	}

	public String getAlias() {
		return alias;
	}

	public QuerySource setAlias(String alias) {
		this.alias = alias;
		return this;
	}

	public String getSource() {
		return source;
	}
	
	public String toString(){
		return source+(alias == null ? "" : " AS "+alias);
	}

	public QueryBody getQuery() {
		return query;
	}

	public boolean isSubQuery(){
		return this.query != null;
	}
	
}
