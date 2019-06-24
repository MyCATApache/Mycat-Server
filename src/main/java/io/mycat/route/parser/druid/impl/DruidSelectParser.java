package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import com.alibaba.druid.sql.dialect.db2.visitor.DB2OutputVisitor;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleOutputVisitor;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGOutputVisitor;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelectQueryBlock;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.wall.spi.WallVisitorUtils;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.route.util.RouterUtil;
import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.sqlengine.mpp.HavingCols;
import io.mycat.sqlengine.mpp.MergeCol;
import io.mycat.sqlengine.mpp.OrderCol;
import io.mycat.util.ObjectUtil;
import io.mycat.util.StringUtil;

public class DruidSelectParser extends DefaultDruidParser {


    protected boolean isNeedParseOrderAgg=true;

    @Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) {
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();

				 parseOrderAggGroupMysql(schema, stmt,rrs, mysqlSelectQuery);
				 //更改canRunInReadDB属性
				 if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false)
				 {
					 rrs.setCanRunInReadDB(false);
				 }

		} else if (sqlSelectQuery instanceof MySqlUnionQuery) { 
//			MySqlUnionQuery unionQuery = (MySqlUnionQuery)sqlSelectQuery;
//			MySqlSelectQueryBlock left = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			MySqlSelectQueryBlock right = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			System.out.println();
		}
	}
	protected void parseOrderAggGroupMysql(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery)
	{
		MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
		stmt.accept(visitor);
//		rrs.setGroupByCols((String[])visitor.getGroupByColumns().toArray());
		if(!isNeedParseOrderAgg)
        {
            return;
        }
		Map<String, String> aliaColumns = parseAggGroupCommon(schema, stmt, rrs, mysqlSelectQuery);

		//setOrderByCols
		if(mysqlSelectQuery.getOrderBy() != null) {
			List<SQLSelectOrderByItem> orderByItems = mysqlSelectQuery.getOrderBy().getItems();
			rrs.setOrderByCols(buildOrderByCols(orderByItems,aliaColumns));
		}
        isNeedParseOrderAgg=false;
	}
	protected Map<String, String> parseAggGroupCommon(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs, SQLSelectQueryBlock mysqlSelectQuery)
	{
		Map<String, String> aliaColumns = new HashMap<String, String>();
		Map<String, Integer> aggrColumns = new HashMap<String, Integer>();
		// Added by winbill, 20160314, for having clause, Begin ==>
		List<String> havingColsName = new ArrayList<String>();
		// Added by winbill, 20160314, for having clause, End  <==
		List<SQLSelectItem> selectList = mysqlSelectQuery.getSelectList();
        boolean isNeedChangeSql=false;
        int size = selectList.size();
        boolean isDistinct=mysqlSelectQuery.getDistionOption()==2;
        for (int i = 0; i < size; i++)
		{
			SQLSelectItem item = selectList.get(i);

			if (item.getExpr() instanceof SQLAggregateExpr)
			{
				SQLAggregateExpr expr = (SQLAggregateExpr) item.getExpr();
				String method = expr.getMethodName();
                boolean isHasArgument=!expr.getArguments().isEmpty();
                if(isHasArgument)
                {
                    String aggrColName = method + "(" + expr.getArguments().get(0) + ")";   // Added by winbill, 20160314, for having clause
                    havingColsName.add(aggrColName);       // Added by winbill, 20160314, for having clause
                }
				//只处理有别名的情况，无别名添加别名，否则某些数据库会得不到正确结果处理
				int mergeType = MergeCol.getMergeType(method);
                if (MergeCol.MERGE_AVG == mergeType&&isRoutMultiNode(schema,rrs))
                {    //跨分片avg需要特殊处理，直接avg结果是不对的
                    String colName = item.getAlias() != null ? item.getAlias() : method + i;
                    SQLSelectItem sum =new SQLSelectItem();
                    String sumColName = colName + "SUM";
                    sum.setAlias(sumColName);
                    SQLAggregateExpr sumExp =new SQLAggregateExpr("SUM");
                    ObjectUtil.copyProperties(expr,sumExp);
                    sumExp.getArguments().addAll(expr.getArguments());
                    sumExp.setMethodName("SUM");
                    sum.setExpr(sumExp);
                    selectList.set(i, sum);
                    aggrColumns.put(sumColName, MergeCol.MERGE_SUM);
                    havingColsName.add(sumColName);    // Added by winbill, 20160314, for having clause
                    havingColsName.add(item.getAlias() != null ? item.getAlias() : "");    // Added by winbill, 20160314, two aliases for AVG

                    SQLSelectItem count =new SQLSelectItem();
                    String countColName = colName + "COUNT";
                    count.setAlias(countColName);
                    SQLAggregateExpr countExp = new SQLAggregateExpr("COUNT");
                    ObjectUtil.copyProperties(expr,countExp);
                    countExp.getArguments().addAll(expr.getArguments());
                    countExp.setMethodName("COUNT");
                    count.setExpr(countExp);
                    selectList.add(count);
                    aggrColumns.put(countColName, MergeCol.MERGE_COUNT);

                    isNeedChangeSql=true;
                    aggrColumns.put(colName, mergeType);
                    rrs.setHasAggrColumn(true);
                } else if (MergeCol.MERGE_UNSUPPORT != mergeType){
					String aggColName = null;
					StringBuilder sb = new StringBuilder();
					if(mysqlSelectQuery instanceof MySqlSelectQueryBlock) {
						expr.accept(new MySqlOutputVisitor(sb));
					} else if(mysqlSelectQuery instanceof OracleSelectQueryBlock) {
						expr.accept(new OracleOutputVisitor(sb));
					} else if(mysqlSelectQuery instanceof PGSelectQueryBlock){
						expr.accept(new PGOutputVisitor(sb));
					} else if(mysqlSelectQuery instanceof SQLServerSelectQueryBlock) {
						expr.accept(new SQLASTOutputVisitor(sb));
					} else if(mysqlSelectQuery instanceof DB2SelectQueryBlock) {
						expr.accept(new DB2OutputVisitor(sb));
					}
					aggColName = sb.toString();

					if (item.getAlias() != null && item.getAlias().length() > 0)
					{
						aggrColumns.put(item.getAlias(), mergeType);
						aliaColumns.put(aggColName,item.getAlias());
					} else
					{   //如果不加，jdbc方式时取不到正确结果   ;修改添加别名
							item.setAlias(method + i);
							aggrColumns.put(method + i, mergeType);
							aliaColumns.put(aggColName, method + i);
                            isNeedChangeSql=true;
					}
					rrs.setHasAggrColumn(true);
					havingColsName.add(item.getAlias());   // Added by winbill, 20160314, for having clause
					havingColsName.add("");                // Added by winbill, 20160314, one alias for non-AVG
				}
			} else
			{
				if (!(item.getExpr() instanceof SQLAllColumnExpr))
				{
					String alia = item.getAlias();
					String field = getFieldName(item);
					if (alia == null)
					{
						alia = field;
					}
					aliaColumns.put(field, alia);
				}
			}

		}
		if(aggrColumns.size() > 0) {
			rrs.setMergeCols(aggrColumns);
		}

        //通过优化转换成group by来实现
        if(isDistinct)
        {
            mysqlSelectQuery.setDistionOption(0);
            SQLSelectGroupByClause   groupBy=new SQLSelectGroupByClause();
            for (String fieldName : aliaColumns.keySet())
            {
                groupBy.addItem(new SQLIdentifierExpr(fieldName));
            }
            mysqlSelectQuery.setGroupBy(groupBy);
            isNeedChangeSql=true;
        }


        //setGroupByCols
		if(mysqlSelectQuery.getGroupBy() != null) {
			List<SQLExpr> groupByItems = mysqlSelectQuery.getGroupBy().getItems();
			String[] groupByCols = buildGroupByCols(groupByItems,aliaColumns);
			rrs.setGroupByCols(groupByCols);
			rrs.setHavings(buildGroupByHaving(mysqlSelectQuery.getGroupBy().getHaving(),aliaColumns));
			rrs.setHasAggrColumn(true);
			rrs.setHavingColsName(havingColsName.toArray()); // Added by winbill, 20160314, for having clause
		}


        if (isNeedChangeSql)
        {
            String sql = stmt.toString();
            rrs.changeNodeSqlAfterAddLimit(schema,getCurentDbType(),sql,0,-1, false);
            getCtx().setSql(sql);
        }
		return aliaColumns;
	}

	private HavingCols buildGroupByHaving(SQLExpr having,Map<String, String> aliaColumns ){
		if (having == null) {
			return null;
		}

		SQLBinaryOpExpr expr  = ((SQLBinaryOpExpr) having);
		SQLExpr left = expr.getLeft();
		SQLBinaryOperator operator = expr.getOperator();
		SQLExpr right = expr.getRight();

		String leftValue = null;;
		if (left instanceof SQLAggregateExpr) {
			leftValue = ((SQLAggregateExpr) left).getMethodName() + "("
					+ ((SQLAggregateExpr) left).getArguments().get(0) + ")";
			String aggrColumnAlias = getAliaColumn(aliaColumns,leftValue);
			if(aggrColumnAlias != null) { // having聚合函数存在别名
				expr.setLeft(new SQLIdentifierExpr(aggrColumnAlias));
				leftValue = aggrColumnAlias;
			}
		} else if (left instanceof SQLIdentifierExpr) {
			leftValue = ((SQLIdentifierExpr) left).getName();
		}

		String rightValue = null;
		if (right instanceof  SQLNumericLiteralExpr) {
			rightValue = right.toString();
		}else if(right instanceof SQLTextLiteralExpr){
			rightValue = StringUtil.removeBackquote(right.toString());
		}

		return new HavingCols(leftValue,rightValue,operator.getName());
	}

  private boolean isRoutMultiNode(SchemaConfig schema,  RouteResultset rrs)
  {
	  if(rrs.getNodes()!=null&&rrs.getNodes().length>1)
	  {
		  return true;
	  }
	  LayerCachePool tableId2DataNodeCache = (LayerCachePool) MycatServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
	  try
	  {
		  tryRoute(schema, rrs, tableId2DataNodeCache);
		  if(rrs.getNodes()!=null&&rrs.getNodes().length>1)
		  {
			  return true;
		  }
	  } catch (SQLNonTransientException e)
	  {
		  throw new RuntimeException(e);
	  }
	  return false;
  }

	private String getFieldName(SQLSelectItem item){
		if ((item.getExpr() instanceof SQLPropertyExpr)||(item.getExpr() instanceof SQLMethodInvokeExpr)
				|| (item.getExpr() instanceof SQLIdentifierExpr) || item.getExpr() instanceof SQLBinaryOpExpr) {			
			return item.getExpr().toString();//字段别名
		}
		else {
			return item.toString();
		}
	}
	
	/**
	 * 现阶段目标为 有一个只涉及到一张表的子查询时,先执行子查询,获得返回结果后,改写原有sql继续执行,得到最终结果.
	 * 在这种情况下,原sql不需要继续解析.
	 * 使用catlet 的情况也不再继续解析.
	 */
	@Override
	public boolean afterVisitorParser(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) {
		int subQuerySize = visitor.getSubQuerys().size();
		
		if(subQuerySize==0&&ctx.getTables().size()==2){ //两表关联,考虑使用catlet
		    if(ctx.getVisitor().getConditions() !=null && ctx.getVisitor().getConditions().size()>0){
		    	return true;
			}
		}else if(subQuerySize==1){     //只涉及一张表的子查询,使用  MiddlerResultHandler 获取中间结果后,改写原有 sql 继续执行 TODO 后期可能会考虑多个.
			SQLSelectQuery sqlSelectQuery = visitor.getSubQuerys().iterator().next().getQuery();
			if(((MySqlSelectQueryBlock)sqlSelectQuery).getFrom() instanceof SQLExprTableSource) {
				return true;
			}
		}
		
		return super.afterVisitorParser(rrs, stmt, visitor);
	}
	
	/**
	 * 改写sql：需要加limit的加上
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException {

		tryRoute(schema, rrs, cachePool);

		rrs.copyLimitToNodes();
		
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			int limitStart = 0;
			int limitSize = schema.getDefaultMaxLimit();

			//clear group having
			SQLSelectGroupByClause groupByClause = mysqlSelectQuery.getGroupBy();
			// Modified by winbill, 20160614, do NOT include having clause when routing to multiple nodes
			if(groupByClause != null && groupByClause.getHaving() != null && isRoutMultiNode(schema,rrs)){
				groupByClause.setHaving(null);
			}
			
			Map<String, Map<String, Set<ColumnRoutePair>>> allConditions = getAllConditions();
			boolean isNeedAddLimit = isNeedAddLimit(schema, rrs, mysqlSelectQuery, allConditions);
			if(isNeedAddLimit) {
				Limit limit = new Limit();
				limit.setRowCount(new SQLIntegerExpr(limitSize));
				mysqlSelectQuery.setLimit(limit);
				rrs.setLimitSize(limitSize);
			    String sql= getSql(rrs, stmt, isNeedAddLimit);
				rrs.changeNodeSqlAfterAddLimit(schema, getCurentDbType(), sql, 0, limitSize, true);

			}
			Limit limit = mysqlSelectQuery.getLimit();
			if(limit != null&&!isNeedAddLimit) {
				SQLIntegerExpr offset = (SQLIntegerExpr)limit.getOffset();
				SQLIntegerExpr count = (SQLIntegerExpr)limit.getRowCount();
				if(offset != null) {
					limitStart = offset.getNumber().intValue();
					rrs.setLimitStart(limitStart);
				} 
				if(count != null) {
					limitSize = count.getNumber().intValue();
					rrs.setLimitSize(limitSize);
				}

				if(isNeedChangeLimit(rrs)) {
					Limit changedLimit = new Limit();
					changedLimit.setRowCount(new SQLIntegerExpr(limitStart + limitSize));
					
					if(offset != null) {
						if(limitStart < 0) {
							String msg = "You have an error in your SQL syntax; check the manual that " +
									"corresponds to your MySQL server version for the right syntax to use near '" + limitStart + "'";
							throw new SQLNonTransientException(ErrorCode.ER_PARSE_ERROR + " - " + msg);
						} else {
							changedLimit.setOffset(new SQLIntegerExpr(0));
							
						}
					}
					
					mysqlSelectQuery.setLimit(changedLimit);

                    String sql= getSql(rrs, stmt, isNeedAddLimit);
					rrs.changeNodeSqlAfterAddLimit(schema,getCurentDbType(),sql,0, limitStart + limitSize, true);

					//设置改写后的sql
					ctx.setSql(sql);

				}   else
				{

                        rrs.changeNodeSqlAfterAddLimit(schema,getCurentDbType(),getCtx().getSql(),rrs.getLimitStart(), rrs.getLimitSize(), true);
					//	ctx.setSql(nativeSql);

				}
				

			}
			
			if(rrs.isDistTable()){
				SQLTableSource from = mysqlSelectQuery.getFrom();

				for (RouteResultsetNode node : rrs.getNodes()) {
					SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
					sqlIdentifierExpr.setParent(from);
					sqlIdentifierExpr.setName(node.getSubTableName());
					SQLExprTableSource from2 = new SQLExprTableSource(sqlIdentifierExpr);
					from2.setAlias(from.getAlias());
					mysqlSelectQuery.setFrom(from2);
					node.setStatement(stmt.toString());
	            }
			}
			
			rrs.setCacheAble(isNeedCache(schema, rrs, mysqlSelectQuery, allConditions));
		}
		
	}
	
	/**
	 * 获取所有的条件：因为可能被or语句拆分成多个RouteCalculateUnit，条件分散了
	 * @return
	 */
	private Map<String, Map<String, Set<ColumnRoutePair>>> getAllConditions() {
		Map<String, Map<String, Set<ColumnRoutePair>>> map = new HashMap<String, Map<String, Set<ColumnRoutePair>>>();
		for(RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
			if(unit != null && unit.getTablesAndConditions() != null) {
				map.putAll(unit.getTablesAndConditions());
			}
		}
		
		return map;
	}

	private void tryRoute(SchemaConfig schema, RouteResultset rrs, LayerCachePool cachePool) throws SQLNonTransientException {
		if(rrs.isFinishedRoute())
		{
			return;//避免重复路由
		}

		//无表的select语句直接路由带任一节点
        if((ctx.getTables() == null || ctx.getTables().size() == 0)&&(ctx.getTableAliasMap()==null||ctx.getTableAliasMap().isEmpty())) {
			rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), ctx.getSql());
			rrs.setFinishedRoute(true);
			return;
		}
//		RouterUtil.tryRouteForTables(schema, ctx, rrs, true, cachePool);
		SortedSet<RouteResultsetNode> nodeSet = new TreeSet<RouteResultsetNode>();
		boolean isAllGlobalTable = RouterUtil.isAllGlobalTable(ctx, schema);
		for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
			RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, ctx, unit, rrs, true, cachePool);
			if (rrsTmp != null&&rrsTmp.getNodes()!=null) {
				for (RouteResultsetNode node : rrsTmp.getNodes()) {
					nodeSet.add(node);
				}
			}
			if(isAllGlobalTable) {//都是全局表时只计算一遍路由
				break;
			}
		}
		
		if(nodeSet.size() == 0) {

            Collection<String> stringCollection= ctx.getTableAliasMap().values() ;
            for (String table : stringCollection)
            {
                if(table!=null&&table.toLowerCase().contains("information_schema."))
                {
                    rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), ctx.getSql());
                    rrs.setFinishedRoute(true);
                    return;
                }
            }
			String msg = " find no Route:" + ctx.getSql();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		
		RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
		int i = 0;
		for (Iterator<RouteResultsetNode> iterator = nodeSet.iterator(); iterator.hasNext();) {
			nodes[i] = (RouteResultsetNode) iterator.next();
			i++;
			
		}
		
		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
	}


	protected String getCurentDbType()
	{
		return JdbcConstants.MYSQL;
	}




	protected String getSql( RouteResultset rrs,SQLStatement stmt, boolean isNeedAddLimit)
	{
		if(getCurentDbType().equalsIgnoreCase("mysql")&&(isNeedChangeLimit(rrs)||isNeedAddLimit))
		{

				return stmt.toString();

		}

	 return getCtx().getSql();
	}


	
	protected boolean isNeedChangeLimit(RouteResultset rrs) {
		if(rrs.getNodes() == null) {
			return false;
		} else {
			if(rrs.getNodes().length > 1) {
				return true;
			}
			return false;
		
		} 
	}
	
	private boolean isNeedCache(SchemaConfig schema, RouteResultset rrs, 
			MySqlSelectQueryBlock mysqlSelectQuery, Map<String, Map<String, Set<ColumnRoutePair>>> allConditions) {
		if(ctx.getTables() == null || ctx.getTables().size() == 0 ) {
			return false;
		}
		TableConfig tc = schema.getTables().get(ctx.getTables().get(0));
		if(tc==null ||(ctx.getTables().size() == 1 && tc.isGlobalTable())
				) {//|| (ctx.getTables().size() == 1) && tc.getRule() == null && tc.getDataNodes().size() == 1
			return false;
		} else {
			//单表主键查询
			if(ctx.getTables().size() == 1) {
				String tableName = ctx.getTables().get(0);
				String primaryKey = schema.getTables().get(tableName).getPrimaryKey();
//				schema.getTables().get(ctx.getTables().get(0)).getParentKey() != null;
				if(ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName) != null
						&& ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName).get(primaryKey) != null 
						&& tc.getDataNodes().size() > 1) {//有主键条件
					return false;
				}
			//全局表不缓存 
			}else if(RouterUtil.isAllGlobalTable(ctx, schema)){
				return false;
			}
			return true;
		}
	}
	
	/**
	 * 单表且是全局表
	 * 单表且rule为空且nodeNodes只有一个
	 * @param schema
	 * @param rrs
	 * @param mysqlSelectQuery
	 * @return
	 */
	private boolean isNeedAddLimit(SchemaConfig schema, RouteResultset rrs, 
			MySqlSelectQueryBlock mysqlSelectQuery, Map<String, Map<String, Set<ColumnRoutePair>>> allConditions) {
//		ctx.getTablesAndConditions().get(key))
		  if(rrs.getLimitSize()>-1)
		  {
			  return false;
		  }else
		if(schema.getDefaultMaxLimit() == -1) {
			return false;
		} else if (mysqlSelectQuery.getLimit() != null) {//语句中已有limit
			return false;
		} else if(ctx.getTables().size() == 1) {
			String tableName = ctx.getTables().get(0);
			TableConfig tableConfig = schema.getTables().get(tableName);
			if(tableConfig==null)
			{
			 return    schema.getDefaultMaxLimit() > -1;   //   找不到则取schema的配置
			}

			boolean isNeedAddLimit= tableConfig.isNeedAddLimit();
			if(!isNeedAddLimit)
			{
				return false;//优先从配置文件取
			}

			if(schema.getTables().get(tableName).isGlobalTable()) {
				return true;
			}

			String primaryKey = schema.getTables().get(tableName).getPrimaryKey();

//			schema.getTables().get(ctx.getTables().get(0)).getParentKey() != null;
			if(allConditions.get(tableName) == null) {//无条件
				return true;
			}
			
			if (allConditions.get(tableName).get(primaryKey) != null) {//条件中带主键
				return false;
			}
			
			return true;
		} else if(rrs.hasPrimaryKeyToCache() && ctx.getTables().size() == 1){//只有一个表且条件中有主键,不需要limit了,因为主键只能查到一条记录
			return false;
		} else {//多表或无表
			return false;
		}
		
	}
	private String getAliaColumn(Map<String, String> aliaColumns,String column ){
		String alia=aliaColumns.get(column);
		if (alia==null){
			if(column.indexOf(".") < 0) {
				String col = "." + column;
				String col2 = ".`" + column+"`";
				//展开aliaColumns，将<c.name,cname>之类的键值对展开成<c.name,cname>和<name,cname>
				for(Map.Entry<String, String> entry : aliaColumns.entrySet()) {
					if(entry.getKey().endsWith(col)||entry.getKey().endsWith(col2)) {
						if(entry.getValue() != null && entry.getValue().indexOf(".") > 0) {
							return column;
						}
						return entry.getValue();
					}
				}
			}
			
			return column;
		}
		else {
			return alia;
		}
	}
	
	private String[] buildGroupByCols(List<SQLExpr> groupByItems,Map<String, String> aliaColumns) {
		String[] groupByCols = new String[groupByItems.size()]; 
		for(int i= 0; i < groupByItems.size(); i++) {
            SQLExpr sqlExpr = groupByItems.get(i);
            String column = null;
            if(sqlExpr instanceof SQLIdentifierExpr )
            {
                column=((SQLIdentifierExpr) sqlExpr).getName();
            } else if(sqlExpr instanceof SQLMethodInvokeExpr){
				column = ((SQLMethodInvokeExpr) sqlExpr).toString();
			} else if(sqlExpr instanceof MySqlOrderingExpr){
                //todo czn
                SQLExpr expr = ((MySqlOrderingExpr) sqlExpr).getExpr();

                if (expr instanceof SQLName)
                {
                    column = StringUtil.removeBackquote(((SQLName) expr).getSimpleName());//不要转大写 2015-2-10 sohudo StringUtil.removeBackquote(expr.getSimpleName().toUpperCase());
                } else
                {
                    column = StringUtil.removeBackquote(expr.toString());
                }
            } else if(sqlExpr instanceof SQLPropertyExpr){
				/**
				 * 针对子查询别名，例如select id from (select h.id from hotnews h  union select h.title from hotnews h ) as t1 group by t1.id;
				 */
				column = sqlExpr.toString();
			}
			if(column == null){
				column = sqlExpr.toString();
			}
			int dotIndex=column.indexOf(".") ;
			int bracketIndex=column.indexOf("(") ;
			//通过判断含有括号来决定是否为函数列
			if(dotIndex!=-1&&bracketIndex==-1)
			{
				//此步骤得到的column必须是不带.的，有别名的用别名，无别名的用字段名
				column=column.substring(dotIndex+1) ;
			}
			groupByCols[i] = getAliaColumn(aliaColumns,column);//column;
		}
		return groupByCols;
	}
	
	protected LinkedHashMap<String, Integer> buildOrderByCols(List<SQLSelectOrderByItem> orderByItems,Map<String, String> aliaColumns) {
		LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
		for(int i= 0; i < orderByItems.size(); i++) {
			SQLOrderingSpecification type = orderByItems.get(i).getType();
            //orderColumn只记录字段名称,因为返回的结果集是不带表名的。
			SQLExpr expr =  orderByItems.get(i).getExpr();
			String col;
			if (expr instanceof SQLName) {
			   col = ((SQLName)expr).getSimpleName();
			}
			else {
				col =expr.toString();
			}
			if(type == null) {
				type = SQLOrderingSpecification.ASC;
			}
			col=getAliaColumn(aliaColumns,col);//此步骤得到的col必须是不带.的，有别名的用别名，无别名的用字段名
			map.put(col, type == SQLOrderingSpecification.ASC ? OrderCol.COL_ORDER_TYPE_ASC : OrderCol.COL_ORDER_TYPE_DESC);
		}
		return map;
	}
	
	private boolean isConditionAlwaysTrue(SQLStatement statement) {
		SQLSelectStatement selectStmt = (SQLSelectStatement)statement;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			SQLExpr expr = mysqlSelectQuery.getWhere();
			
			Object o = WallVisitorUtils.getValue(expr);
			if(Boolean.TRUE.equals(o)) {
				return true;
			}
			return false;
		} else {//union
			return false;
		}
		
	}

	protected void setLimitIFChange(SQLStatement stmt, RouteResultset rrs, SchemaConfig schema, SQLBinaryOpExpr one, int firstrownum, int lastrownum)
	{
		rrs.setLimitStart(firstrownum);
		rrs.setLimitSize(lastrownum - firstrownum);
		LayerCachePool tableId2DataNodeCache = (LayerCachePool) MycatServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
		try
		{
			tryRoute(schema, rrs, tableId2DataNodeCache);
		} catch (SQLNonTransientException e)
		{
			throw new RuntimeException(e);
		}
		if (isNeedChangeLimit(rrs))
		{
			one.setRight(new SQLIntegerExpr(0));
            String curentDbType ="db2".equalsIgnoreCase(this.getCurentDbType())?"oracle":getCurentDbType();
            String sql =   SQLUtils.toSQLString(stmt, curentDbType);;
			rrs.changeNodeSqlAfterAddLimit(schema,getCurentDbType(), sql,0,lastrownum, false);
			//设置改写后的sql
			getCtx().setSql(sql);
		}
	}
}
