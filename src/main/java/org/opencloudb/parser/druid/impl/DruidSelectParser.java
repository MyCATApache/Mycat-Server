package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.LinkedHashMap;
import java.util.List;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mpp.OrderCol;
import org.opencloudb.route.RouteResultset;

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
	public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			int limitStart = 0;
			int limitSize = schema.getDefaultMaxLimit();
			if(isNeedAddLimit(schema, rrs, mysqlSelectQuery)  ) {
				Limit limit = new Limit();
				limit.setRowCount(new SQLIntegerExpr(limitSize));
				mysqlSelectQuery.setLimit(limit);
			}
			Limit limit = mysqlSelectQuery.getLimit();
			if(limit != null) {
				SQLIntegerExpr offset = (SQLIntegerExpr)limit.getOffset();
				SQLIntegerExpr count = (SQLIntegerExpr)limit.getRowCount();
				if(offset != null) {
					limitStart = offset.getNumber().intValue();
				} 
				if(count != null) {
					limitSize = count.getNumber().intValue();
				}
				
				Limit changedLimit = new Limit();
				changedLimit.setRowCount(new SQLIntegerExpr(limitStart + limitSize));
				changedLimit.setOffset(new SQLIntegerExpr(0));
				mysqlSelectQuery.setLimit(changedLimit);
				//设置改写后的sql
				ctx.setSql(stmt.toString());
				
				rrs.setLimitStart(limitStart);
				rrs.setLimitSize(limitSize);
			}
		}
	}
	
	private boolean isNeedAddLimit(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery) {
		if (!rrs.hasPrimaryKeyToCache() && schema.getDefaultMaxLimit() != -1 
				&& mysqlSelectQuery.getLimit() == null && ctx.getTables().size() > 0) {
			return true;
		}
		return false;
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
