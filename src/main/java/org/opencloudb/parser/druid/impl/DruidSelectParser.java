package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.LinkedHashMap;
import java.util.List;

import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.mpp.OrderCol;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlSelectGroupByExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;

public class DruidSelectParser extends DefaultDruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) {
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			//setHasAggrColumn ,such as count(*)
			
			//以下注释的代码没准以后有用，rrs.setMergeCols(aggrColumns);目前就是个坑，设置了反而报错，得不到正确结果
//			boolean hasAggrColumn = false;
//			Map<String, Integer> aggrColumns = new HashMap<String, Integer>();
//			for(SQLSelectItem item : mysqlSelectQuery.getSelectList()) {
//				
//				if(item.getExpr() instanceof SQLAggregateExpr) {
//					SQLAggregateExpr expr = (SQLAggregateExpr)item.getExpr();
//					List<SQLExpr> argList = expr.getArguments();
//					String method = expr.getMethodName();
//					if (argList.size() > 0) {
//						for(SQLExpr arg : argList) {
//							aggrColumns.put(arg.toString(), MergeCol.getMergeType(method));
//						}
//					}
//					hasAggrColumn = true;
//				}
//				
//			}
//			if(aggrColumns.size() > 0) {
//				rrs.setMergeCols(aggrColumns);
//			}
//			if(hasAggrColumn) {
//				rrs.setHasAggrColumn(true);
//			}
			
			
			//setHasAggrColumn ,such as count(*)
			for(SQLSelectItem item : mysqlSelectQuery.getSelectList()) {
				if(item.getExpr() instanceof SQLAggregateExpr) {
					rrs.setHasAggrColumn(true);
					break;
				}
			}
			
			//setGroupByCols
			if(mysqlSelectQuery.getGroupBy() != null) {
				List<SQLExpr> groupByItems = mysqlSelectQuery.getGroupBy().getItems();
				String[] groupByCols = buildGroupByCols(groupByItems);
				rrs.setGroupByCols(groupByCols);
			}
			
			//setOrderByCols
			if(mysqlSelectQuery.getOrderBy() != null) {
				List<SQLSelectOrderByItem> orderByItems = mysqlSelectQuery.getOrderBy().getItems();
				rrs.setOrderByCols(buildOrderByCols(orderByItems));
			}
			
			//setMergeCols TODO 目前设置这个值会报错，可能有特殊场景需要，后续如果出现bug在考虑设置

		} else if (sqlSelectQuery instanceof MySqlUnionQuery) { //TODO union语句可能需要额外考虑，目前不处理也没问题
//			MySqlUnionQuery unionQuery = (MySqlUnionQuery)sqlSelectQuery;
//			MySqlSelectQueryBlock left = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			MySqlSelectQueryBlock right = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			System.out.println();
		}
	}
	
	/**
	 * 改写sql：需要加limit的加上
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException {
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
					rrs.changeNodeSqlAfterAddLimit(stmt.toString());
//					rrs.setSqlChanged(true);
				}
				
				
				//设置改写后的sql
				ctx.setSql(stmt.toString());
			}
			
			rrs.setCacheAble(isNeedCache(schema, rrs, mysqlSelectQuery));
		}
		
	}
	
	private boolean isNeedChangeLimit(RouteResultset rrs, SchemaConfig schema) {
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
			if(schema.getTables().get(ctx.getTables().get(0)).isGlobalTable()) {
				return true;//TODO
			}
			String tableName = ctx.getTables().get(0);
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
	
	private String[] buildGroupByCols(List<SQLExpr> groupByItems) {
		String[] groupByCols = new String[groupByItems.size()]; 
		for(int i= 0; i < groupByItems.size(); i++) {
			String column = removeBackquote(((MySqlSelectGroupByExpr)groupByItems.get(i)).getExpr().toString().toUpperCase());
			groupByCols[i] = column;
		}
		return groupByCols;
	}
	
	private LinkedHashMap<String, Integer> buildOrderByCols(List<SQLSelectOrderByItem> orderByItems) {
		LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
		for(int i= 0; i < orderByItems.size(); i++) {
			SQLOrderingSpecification type = orderByItems.get(i).getType();
			String col =  orderByItems.get(i).getExpr().toString();
			if(type == null) {
				type = SQLOrderingSpecification.ASC;
			}
			map.put(col, type == SQLOrderingSpecification.ASC ? OrderCol.COL_ORDER_TYPE_ASC : OrderCol.COL_ORDER_TYPE_DESC);
		}
		return map;
	}
}
