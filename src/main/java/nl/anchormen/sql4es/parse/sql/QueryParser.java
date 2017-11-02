package nl.anchormen.sql4es.parse.sql;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SortItem;

import nl.anchormen.sql4es.model.BasicQueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Column.Operation;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.OrderBy;
import nl.anchormen.sql4es.model.QuerySource;
import nl.anchormen.sql4es.model.Utils;
import nl.anchormen.sql4es.model.expression.ColumnReference;
import nl.anchormen.sql4es.model.expression.ICalculation;
import nl.anchormen.sql4es.model.expression.IComparison;
import nl.anchormen.sql4es.model.expression.SimpleCalculation;

/**
 * Interprets the parsed query and build the appropriate ES query (a {@link SearchRequestBuilder} instance). 
 * The other parses within this package are used to parse their speicific clause (WHERE, HAVING etc)
 *  
 * @author cversloot
 *
 */
public class QueryParser extends AstVisitor<ParseResult, Object>{
	
	private final static SelectParser selectParser = new SelectParser();
	private final static WhereParser whereParser = new WhereParser();
	private final static HavingParser havingParser = new HavingParser();
	private final static RelationParser relationParser = new RelationParser();
	private final static GroupParser groupParser = new GroupParser();
	private final static OrderByParser orderOarser = new OrderByParser();
	
	private String sql;
	private int maxRows = -1;
	private Heading heading = new Heading();
	private Properties props;
	private Map<String, Map<String, Integer>> tableColumnInfo;
	
	/**
	 * Builds the provided {@link SearchRequestBuilder} by parsing the {@link Query} using the properties provided.
	 * @param sql the original sql statement
	 * @param queryBody the Query parsed from the sql
	 * @param searchReq the request to build
	 * @param props a set of properties to use in certain cases
	 * @param tableColumnInfo mapping from available tables to columns and their typesd
	 * @return an array containing [ {@link Heading}, {@link IComparison} having, List&lt;{@link OrderBy}&gt; orderings, Integer limit]
	 * @throws SQLException
	 */
	public ParseResult parse(String sql, QueryBody queryBody, int maxRows, 
			Properties props, Map<String, Map<String, Integer>> tableColumnInfo) throws SQLException{
		this.sql = sql.replace("\r", " ").replace("\n", " ");// TODO: this removes linefeeds from string literals as well!
		this.props = props;
		this.maxRows = maxRows;
		this.tableColumnInfo = tableColumnInfo;
		
		if(queryBody instanceof QuerySpecification){
			ParseResult result = queryBody.accept(this, null);
			if(result.getException() != null) throw result.getException();
			return result;
		}
		throw new SQLException("The provided query does not contain a QueryBody");
	}
	
	@Override
	protected ParseResult visitQuerySpecification(QuerySpecification node, Object obj){
		this.heading = new Heading();
		BasicQueryState state = new BasicQueryState(sql, heading, props);
		int limit = -1;
		AggregationBuilder aggregation = null;
		QueryBuilder query = null;
		IComparison having = null;
		List<OrderBy> orderings = new ArrayList<OrderBy>();
		boolean useCache = false;
		ParseResult subQuery = null;
		
		// check for distinct in combination with group by
		if(node.getSelect().isDistinct() && !node.getGroupBy().isEmpty()){
			state.addException("Unable to combine DISTINCT and GROUP BY within a single query");
			return new ParseResult(state.getException());
		};
		
		// get limit (possibly used by other parsers)
		if(node.getLimit().isPresent()){
			limit = Integer.parseInt(node.getLimit().get());
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		// get sources to fetch data from
		if(node.getFrom().isPresent()){
			SourcesResult sr = getSources(node.getFrom().get(), state);
			useCache = sr.useCache;
			subQuery = sr.subQueryInfo;
		}
		
		// get columns to fetch (builds the header)
		for(SelectItem si : node.getSelect().getSelectItems()){
			si.accept(selectParser, state);
		}
		if(state.hasException()) return new ParseResult(state.getException());
		boolean requestScore = heading.hasLabel("_score");
		
		// Translate column references and their aliases back to their case sensitive forms
		heading.reorderAndFixColumns(this.sql, "select.+", ".+from");
		
		// create aggregation in case of DISTINCT
		if(node.getSelect().isDistinct()){
			aggregation = groupParser.addDistinctAggregation(state);
		}

		// add a Query
		query = QueryBuilders.matchAllQuery();
		if(node.getWhere().isPresent()){
			query = whereParser.parse(node.getWhere().get(), state);
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		// parse group by and create aggregations accordingly
		if(node.getGroupBy() != null && node.getGroupBy().size() > 0){
			aggregation = groupParser.parse(node.getGroupBy(), state);
		}else if(heading.aggregateOnly()){
			aggregation = groupParser.buildFilterAggregation(query, heading);
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		// parse Having (is executed client side after results have been fetched)
		if(node.getHaving().isPresent()){
			having = node.getHaving().get().accept(havingParser, state);
		}

		// parse ORDER BY
		if(!node.getOrderBy().isEmpty()){
			for(SortItem si : node.getOrderBy()){
				OrderBy ob = si.accept(orderOarser, state);
				if(state.hasException()) return new ParseResult(state.getException());
				orderings.add(ob);
			}
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		ParseResult result = new ParseResult(heading, state.getSources(), query, aggregation, having, orderings, limit, useCache, requestScore);
		if(subQuery != null)try{
			if(subQuery.getAggregation() == null && result.getAggregation() == null)
				result = mergeSelectWithSelect(result, subQuery);
			else if(subQuery.getAggregation() != null && result.getAggregation() == null)
				result = mergeSelectWithAgg(result, subQuery);
			else if(subQuery.getAggregation() == null && result.getAggregation() != null){
				result = mergeAggWithSelect(result, subQuery);
				
				if(result.getHeading().aggregateOnly()){
					AggregationBuilder agg = groupParser.buildFilterAggregation(result.getQuery(), result.getHeading());
					result.setAggregation(agg).setQuery(null);
				}else{
					BasicQueryState state2 = new BasicQueryState(sql, result.getHeading(), props);
					AggregationBuilder agg = groupParser.addDistinctAggregation(state2);
					result.setAggregation(agg);
				}
			}else throw new SQLException("Unable to merge the Sub query with the top query");
		}catch(SQLException e){
			return new ParseResult(e);
		}
		return result;
	}

	/**
	 * Gets the sources to query from the provided Relation. Parsed relations are put inside the state
	 * @param relation
	 * @param state
	 * @param searchReq
	 * @return if the set with relations contains the query cache identifier
	 */
	private SourcesResult getSources(Relation relation, BasicQueryState state){
		List<QuerySource> sources = relation.accept(relationParser, state);
		boolean useCache = false;
		ParseResult subQueryInfo = null;
		if(state.hasException()) return new SourcesResult(false, null);
		if(sources.size() < 1) {
			state.addException("Specify atleast one valid table to execute the query on!");
			return new SourcesResult(false, null);
		}
		for(int i=0; i<sources.size(); i++){
			if(sources.get(i).getSource().toLowerCase().equals(props.getProperty(Utils.PROP_QUERY_CACHE_TABLE, "query_cache"))){
				useCache = true;
				sources.remove(i);
				i--;
			}else if(sources.get(i).isSubQuery()){
				QuerySource qs = sources.get(i);
				QueryParser subQueryParser = new QueryParser();
				try {
					subQueryInfo = subQueryParser.parse(qs.getSource(), qs.getQuery(), maxRows, props, tableColumnInfo);
				} catch (SQLException e) {
					state.addException("Unable to parse sub-query due to: "+e.getMessage());
				}
				//sources.remove(i);
				//i--;
			}
		}
		heading.setTypes(this.typesForColumns(sources));
		state.setRelations(sources);
		return new SourcesResult(useCache, subQueryInfo);
	}

	/**
	 * Merges two nested SELECT queries 'SELECT a as b from (select myfield as a FROM mytype)'
	 * @param top the top SELECT
	 * @param nested the nested SELECT
	 * @return a new ParseResult in which the two selects has been merged
	 * @throws SQLException in case this function is unable to marge the two queries
	 */
	private ParseResult mergeSelectWithSelect(ParseResult top, ParseResult nested) throws SQLException{
		int limit = Math.min(top.getLimit(), nested.getLimit());
		if(limit <= 0) limit = Math.max(top.getLimit(), nested.getLimit());
		List<OrderBy> sorts = nested.getSorts();
		sorts.addAll(top.getSorts());
		QueryBuilder query = QueryBuilders.boolQuery().must(top.getQuery()).must(nested.getQuery());
		boolean score = top.getRequestScore() || nested.getRequestScore();
		boolean useCache = top.getUseCache() || nested.getUseCache();
		Heading head = new Heading();
		if(top.getHeading().hasAllCols()) head = nested.getHeading();
		else{
			for(Column col : top.getHeading().columns()){
				Column col2 = nested.getHeading().getColumnByNameAndOp(col.getColumn(), Operation.NONE);
				if(col2 == null) col2 = nested.getHeading().getColumnByLabel(col.getAlias());
				if(col2 == null) throw new SQLException("Unable to determine column '"+col.getLabel()+"' within nested query");
				String alias = (col.getAlias() == null ? col.getColumn() : col.getAlias());
				head.add(new Column(col2.getColumn()).setAlias(alias).setSqlType(col2.getSqlType()));
			}
		}
		return new ParseResult(head, nested.getSources(), query, null, null, sorts, limit, useCache, score);
	}
	
	/**
	 * Merges a top level SELECT query with its nested AGGREGATION
	 * @param top
	 * @param nested
	 * @return
	 * @throws SQLException
	 */
	private ParseResult mergeSelectWithAgg(ParseResult top, ParseResult nested) throws SQLException{
		if(top.getRequestScore()) throw new SQLException("Unable to request a _score on an aggregation");
		if(!(top.getQuery() instanceof MatchAllQueryBuilder)) throw new SQLException("Unable to combine a WHERE clause with a nested query");
		int limit = Math.min(top.getLimit(), nested.getLimit());
		if(limit <= 0) limit = Math.max(top.getLimit(), nested.getLimit());
		List<OrderBy> sorts = nested.getSorts();
		sorts.addAll(top.getSorts());
		boolean useCache = top.getUseCache() || nested.getUseCache();
		QueryBuilder aggQuery = nested.getQuery();
		AggregationBuilder agg = nested.getAggregation();
		IComparison having = nested.getHaving();
		
		Heading head = new Heading();
		if(top.getHeading().hasAllCols()) head = nested.getHeading();
		else{
			for(Column col : top.getHeading().columns()){
				Column col2 = nested.getHeading().getColumnByNameAndOp(col.getColumn(), Operation.NONE);
				if(col2 == null) col2 = nested.getHeading().getColumnByLabel(col.getAlias());
				if(col2 == null) throw new SQLException("Unable to determine column '"+col+"' within nested query");
				nested.getHeading().remove(col2);
				head.add(new Column(col2.getColumn(), col2.getOp()).setAlias(col.getAlias())
						.setSqlType(col2.getSqlType()));
			}
			for(Column col2 : nested.getHeading().columns()){
				head.add(new Column(col2.getColumn(), col2.getOp())
					.setAlias(col2.getAlias())
					.setCalculation(col2.getCalculation()).setSqlType(col2.getSqlType())
					.setTable(col2.getTable(), col2.getTableAlias()).setVisible(false)
				);
			}
		}
		head.buildIndex();
		return new ParseResult(head, nested.getSources(), aggQuery, agg, having, sorts, limit, useCache, false);
	}
	
	/**
	 * Merges a top level aggregation query with an inner select
	 * @param result
	 * @param subQuery
	 * @return
	 * @throws SQLException 
	 */
	private ParseResult mergeAggWithSelect(ParseResult top, ParseResult nested) throws SQLException {
		if(nested.getRequestScore()) throw new SQLException("Unable to request a _score on an aggregation");
		int limit = top.getLimit();
		List<OrderBy> sorts = top.getSorts();
		boolean useCache = top.getUseCache() || nested.getUseCache();
		
		QueryBuilder query = top.getQuery();
		if(query instanceof MatchAllQueryBuilder) query = nested.getQuery();
		else if(!(nested.getQuery() instanceof MatchAllQueryBuilder)) query = QueryBuilders.boolQuery().must(top.getQuery()).must(nested.getQuery());
		
		AggregationBuilder agg = top.getAggregation();
		IComparison having = top.getHaving();
		Heading head = new Heading();
		if(nested.getHeading().hasAllCols()){
			head = top.getHeading();
		}else{
			for(Column col : top.getHeading().columns()){
				if(col.hasCalculation()){
					translateCalculation(col.getCalculation(), nested.getHeading());
					head.add(new Column(col.getColumn(), col.getOp()).setAlias(col.getAlias())
							.setCalculation(col.getCalculation()).setSqlType(Types.FLOAT));
				}else{
					Column col2 = nested.getHeading().getColumnByNameAndOp(col.getColumn(), Operation.NONE);
					if(col2 == null) col2 = nested.getHeading().getColumnByLabel(col.getAlias());
					if(col2 == null && col.getOp() == Operation.COUNT){
						head.add(col);
						continue;
					}else if(col2 == null) throw new SQLException("Unable to determine column '"+col.getLabel()+"' within nested query");
					String alias = (col.getAlias() == null ? col.getColumn() : col.getAlias());
					head.add(new Column(col2.getColumn(), col.getOp()).setAlias(alias).setVisible(col.isVisible())
							.setSqlType(col2.getSqlType()));
				}
			}
		}
		head.buildIndex();
		return new ParseResult(head, nested.getSources(), query, agg, having, sorts, limit, useCache, false);
	}
	
	/**
	 * Traverses a {@link ICalculation} tree to fix column references pointing to a nested query
	 * @param calc
	 * @param top
	 * @param nested
	 * @throws SQLException
	 */
	private void translateCalculation(ICalculation calc, Heading nested) throws SQLException{
		if(calc instanceof ColumnReference){
			Column col = ((ColumnReference)calc).getColumn();
			Column col2 = nested.getColumnByNameAndOp(col.getColumn(), Operation.NONE);
			if(col2 == null) col2 = nested.getColumnByLabel(col.getAlias());
			if(col2 != null){
				col.setColumn(col2.getColumn());
			}
		}else if(calc instanceof SimpleCalculation){
			SimpleCalculation sc = (SimpleCalculation)calc;
			translateCalculation(sc.left(), nested);
			translateCalculation(sc.right(), nested);
		}
	}
	
	/**
	 * Gets SQL column types for the provided tables as a map from colname to java.sql.Types
	 * @param tables
	 * @return
	 */
	public Map<String, Integer> typesForColumns(List<QuerySource> relations){
		HashMap<String, Integer> colType = new HashMap<String, Integer>();
		colType.put(Heading.ID, Types.VARCHAR);
		colType.put(Heading.TYPE, Types.VARCHAR);
		colType.put(Heading.INDEX, Types.VARCHAR);
		for(QuerySource table : relations){
			if(!tableColumnInfo.containsKey(table.getSource())) continue;
			colType.putAll( tableColumnInfo.get(table.getSource()) );
		}
		return colType;
	}

	private class SourcesResult {
		public boolean useCache;
		public ParseResult subQueryInfo;
		
		public SourcesResult(boolean useCache, ParseResult subQueryInfo){
			this.useCache = useCache;
			this.subQueryInfo = subQueryInfo;
		}
	}
}

