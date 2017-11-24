package nl.anchormen.sql4es.parse.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.OrderBy;
import nl.anchormen.sql4es.model.QuerySource;
import nl.anchormen.sql4es.model.expression.IComparison;

public class ParseResult {

	private Heading heading;
	private List<QuerySource> sources;
	private QueryBuilder query;
	private AggregationBuilder aggregation;
	private IComparison having;
	private List<OrderBy> sorts = new ArrayList<OrderBy>();
	private int limit = -1;
	private Boolean useCache = false;
	private Boolean requestScore = false;
	private SQLException exception;
	
	public ParseResult(Heading heading, List<QuerySource> sources, QueryBuilder query, AggregationBuilder aggregation,
			IComparison having, List<OrderBy> sorts, Integer limit, Boolean useCache, Boolean requestScore) {
		super();
		this.heading = heading;
		this.sources = sources;
		this.query = query;
		this.aggregation = aggregation;
		this.having = having;
		if(sorts != null) this.sorts = sorts;
		this.limit = limit;
		this.useCache = useCache;
		this.requestScore = requestScore;
	}
	
	public ParseResult(SQLException exception){
		this.exception = exception;
	}

	public Heading getHeading() {
		return heading;
	}

	public List<QuerySource> getSources() {
		return sources;
	}

	public QueryBuilder getQuery() {
		return query;
	}

	public AggregationBuilder getAggregation() {
		return aggregation;
	}

	public IComparison getHaving() {
		return having;
	}

	public List<OrderBy> getSorts() {
		return sorts;
	}

	public Integer getLimit() {
		return limit;
	}

	public Boolean getUseCache() {
		return useCache;
	}

	public Boolean getRequestScore() {
		return requestScore;
	}

	public SQLException getException() {
		return exception;
	}

	public ParseResult setHeading(Heading heading) {
		this.heading = heading;
		return this;
	}

	public ParseResult setSources(List<QuerySource> sources) {
		this.sources = sources;
		return this;
	}

	public ParseResult setQuery(QueryBuilder query) {
		this.query = query;
		return this;
	}

	public ParseResult setAggregation(AggregationBuilder aggregation) {
		this.aggregation = aggregation;
		return this;
	}

	public ParseResult setHaving(IComparison having) {
		this.having = having;
		return this;
	}

	public ParseResult setSorts(List<OrderBy> sorts) {
		this.sorts = sorts;
		return this;
	}

	public ParseResult setLimit(int limit) {
		this.limit = limit;
		return this;
	}

	public ParseResult setUseCache(Boolean useCache) {
		this.useCache = useCache;
		return this;
	}

	public ParseResult setRequestScore(Boolean requestScore) {
		this.requestScore = requestScore;
		return this;
	}

	public void setException(SQLException exception) {
		this.exception = exception;
	}

}
