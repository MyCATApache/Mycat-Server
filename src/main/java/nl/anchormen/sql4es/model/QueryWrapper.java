package nl.anchormen.sql4es.model;

import org.elasticsearch.index.query.QueryBuilder;

public class QueryWrapper {

	private QueryBuilder query;
	private String nestField = null;
	
	public QueryWrapper(QueryBuilder query) {
		this.query = query;
	}

	public QueryWrapper(QueryBuilder query, String nestField) {
		super();
		this.query = query;
		this.nestField = nestField;
	}

	public QueryBuilder getQuery() {
		return query;
	}

	public String getNestField() {
		return nestField;
	}
	
	
	
}
