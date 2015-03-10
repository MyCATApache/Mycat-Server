package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlSelectGroupByExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
import com.alibaba.druid.wall.spi.WallVisitorUtils;

import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.mpp.MergeCol;
import org.opencloudb.mpp.OrderCol;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;

import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DruidSelectParser extends DefaultDruidParser {


	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) {
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();

				 parseOrderAggGroupMysql(rrs, mysqlSelectQuery);
				 //更改canRunInReadDB属性
				 if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false)
				 {
					 rrs.setCanRunInReadDB(false);
				 }

		} else if (sqlSelectQuery instanceof MySqlUnionQuery) { //TODO union语句可能需要额外考虑，目前不处理也没问题
//			MySqlUnionQuery unionQuery = (MySqlUnionQuery)sqlSelectQuery;
//			MySqlSelectQueryBlock left = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			MySqlSelectQueryBlock right = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			System.out.println();
		}
	}
	protected void parseOrderAggGroupMysql(RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery)
	{
		Map<String, String> aliaColumns = parseAggGroupCommon(rrs, mysqlSelectQuery);

		//setOrderByCols
		if(mysqlSelectQuery.getOrderBy() != null) {
			List<SQLSelectOrderByItem> orderByItems = mysqlSelectQuery.getOrderBy().getItems();
			rrs.setOrderByCols(buildOrderByCols(orderByItems,aliaColumns));
		}
	}
	protected Map<String, String> parseAggGroupCommon(RouteResultset rrs, SQLSelectQueryBlock mysqlSelectQuery)
	{
		Map<String, String> aliaColumns = new HashMap<String, String>();//sohudo 2015-2-5 解决了下面这个坑
		//以下注释的代码没准以后有用，rrs.setMergeCols(aggrColumns);目前就是个坑，设置了反而报错，得不到正确结果
		//setHasAggrColumn ,such as count(*)
		Map<String, Integer> aggrColumns = new HashMap<String, Integer>();
		for(SQLSelectItem item : mysqlSelectQuery.getSelectList()) {

			if(item.getExpr() instanceof SQLAggregateExpr) {
				SQLAggregateExpr expr = (SQLAggregateExpr)item.getExpr();
				String method = expr.getMethodName();
				//只处理有别名的情况，无别名丢给DataMergeService.onRowMetaData处理
				if (item.getAlias() != null && item.getAlias().length() > 0) {
					aggrColumns.put(item.getAlias(), MergeCol.getMergeType(method));
				}
				rrs.setHasAggrColumn(true);
			}
			else{
				if (!(item.getExpr() instanceof SQLAllColumnExpr)) {
					String alia=item.getAlias();
					String field=getFieldName(item);
					if (alia==null){
						alia=field;
					}
					aliaColumns.put(field,alia);
				}
			}

		}
		if(aggrColumns.size() > 0) {
			rrs.setMergeCols(aggrColumns);
		}

		//setGroupByCols
		if(mysqlSelectQuery.getGroupBy() != null) {
			List<SQLExpr> groupByItems = mysqlSelectQuery.getGroupBy().getItems();
			String[] groupByCols = buildGroupByCols(groupByItems,aliaColumns);
			rrs.setGroupByCols(groupByCols);
			rrs.setHasAggrColumn(true);
		}
		return aliaColumns;
	}


	private String getFieldName(SQLSelectItem item){
		if ((item.getExpr() instanceof SQLPropertyExpr)||(item.getExpr() instanceof SQLMethodInvokeExpr)
				|| (item.getExpr() instanceof SQLIdentifierExpr)) {			
			return item.getExpr().toString();//字段别名
		}
		else
		  return item.toString();
	}
	/**
	 * 改写sql：需要加limit的加上
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException {
		if(isConditionAlwaysTrue(stmt)) {
			ctx.clear();
		}
		//无表的select语句直接路由带任一节点
		if(ctx.getTables() == null || ctx.getTables().size() == 0) {
			rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), ctx.getSql());
			rrs.setFinishedRoute(true);
			return;
		}
		RouterUtil.tryRouteForTables(schema, ctx, rrs, true,cachePool);
		if(rrs == null) {
			String msg = " find no Route:" + ctx.getSql();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		rrs.setFinishedRoute(true);
		
//		if(!isNeedChangeLimit(rrs,schema)){
//			return;
//		}
		
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			int limitStart = 0;
			int limitSize = schema.getDefaultMaxLimit();
			boolean isNeedAddLimit = isNeedAddLimit(schema, rrs, mysqlSelectQuery);
			if(isNeedAddLimit) {
				Limit limit = new Limit();
				limit.setRowCount(new SQLIntegerExpr(limitSize));
				mysqlSelectQuery.setLimit(limit);
				rrs.changeNodeSqlAfterAddLimit(stmt.toString());
				String nativeSql=convertToNativePageSql(selectStmt.toString(),0,limitSize);
				rrs.changeNodeSqlAfterAddLimit(nativeSql);
			}
			Limit limit = mysqlSelectQuery.getLimit();
			if(limit != null) {
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

				if(isNeedChangeLimit(rrs, schema)) {
					Limit changedLimit = new Limit();
					changedLimit.setRowCount(new SQLIntegerExpr(limitStart + limitSize));
					
					if(offset != null) {
						if(limitStart < 0) {
							String msg = "You have an error in your SQL syntax; check the manual that " +
									"corresponds to your MySQL server version for the right syntax to use near '" + limitStart + "'";
							throw new SQLNonTransientException(ErrorCode.ER_PARSE_ERROR + " - " + msg);
						} else {
							changedLimit.setOffset(new SQLIntegerExpr(0));
							//TODO
						}
					}
					
					mysqlSelectQuery.setLimit(changedLimit);
					String nativeSql=convertToNativePageSql(selectStmt.toString(),0,limitStart + limitSize);
					rrs.changeNodeSqlAfterAddLimit(nativeSql);
//					rrs.setSqlChanged(true);
				}   else
				{
					//单节点也需要转换limit
					String nativeSql=convertToNativePageSql(selectStmt.toString(),rrs.getLimitStart(),rrs.getLimitSize());
					rrs.changeNodeSqlAfterAddLimit(nativeSql);
				}
				
				
				//设置改写后的sql
				ctx.setSql(stmt.toString());
			}
			
			rrs.setCacheAble(isNeedCache(schema, rrs, mysqlSelectQuery));
		}
		
	}


	protected String  convertToNativePageSql( String sql,int offset,int count)
	{
	 return sql;
	}


	
	protected boolean isNeedChangeLimit(RouteResultset rrs, SchemaConfig schema) {
		if(rrs.getNodes() == null) {
			return false;
		} else {
			if(rrs.getNodes().length > 1) {
				return true;
			}
			return false;
		
		} 
	}
	
	private boolean isNeedCache(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery) {
		if(ctx.getTables() == null || ctx.getTables().size() == 0 ) {
			return false;
		}
		TableConfig tc = schema.getTables().get(ctx.getTables().get(0));
		if((ctx.getTables().size() == 1 && tc.isGlobalTable())
				) {//|| (ctx.getTables().size() == 1) && tc.getRule() == null && tc.getDataNodes().size() == 1
			return false;
		} else {
			//单表主键查询
			if(ctx.getTables().size() == 1) {
				String tableName = ctx.getTables().get(0);
				String primaryKey = schema.getTables().get(tableName).getPrimaryKey();
//				schema.getTables().get(ctx.getTables().get(0)).getParentKey() != null;
				if(ctx.getTablesAndConditions().get(tableName) != null
						&& ctx.getTablesAndConditions().get(tableName).get(primaryKey) != null 
						&& tc.getDataNodes().size() > 1) {//有主键条件
					return false;
				} 
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
	private boolean isNeedAddLimit(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery) {
//		ctx.getTablesAndConditions().get(key))
		
		if(schema.getDefaultMaxLimit() == -1) {
			return false;
		} else if (mysqlSelectQuery.getLimit() != null) {//语句中已有limit
			return false;
		} else if(ctx.getTables().size() == 1) {
			String tableName = ctx.getTables().get(0);
			boolean isNeedAddLimit=schema.getTables().get(tableName).isNeedAddLimit();
			if(!isNeedAddLimit)
			{
				return false;//优先从配置文件取
			}

			if(schema.getTables().get(tableName).isGlobalTable()) {
				return true;//TODO
			}

			String primaryKey = schema.getTables().get(tableName).getPrimaryKey();

//			schema.getTables().get(ctx.getTables().get(0)).getParentKey() != null;
			if(ctx.getTablesAndConditions().get(tableName) == null) {//无条件
				return true;
			}
			
			if (ctx.getTablesAndConditions().get(tableName).get(primaryKey) != null) {//条件中带主键
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
			return column;
		}
		else {
			return alia;
		}
	}
	
	private String[] buildGroupByCols(List<SQLExpr> groupByItems,Map<String, String> aliaColumns) {
		String[] groupByCols = new String[groupByItems.size()]; 
		for(int i= 0; i < groupByItems.size(); i++) {
			SQLExpr expr = ((MySqlSelectGroupByExpr)groupByItems.get(i)).getExpr();			
			String column; 
			if (expr instanceof SQLName) {
				column= removeBackquote(((SQLName)expr).getSimpleName());//不要转大写 2015-2-10 sohudo removeBackquote(expr.getSimpleName().toUpperCase());
			}
			else {
				column= removeBackquote(expr.toString());
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
			col=getAliaColumn(aliaColumns,col);
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
}
