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
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelect;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
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
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DruidSelectOracleParser extends DruidSelectParser {

	protected boolean isNeedParseOrderAgg=true;

	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) {
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();

		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			Limit limit=mysqlSelectQuery.getLimit();
			if(limit==null)
			{
				  //使用oracle的解析，否则会有部分oracle语法识别错误
				  OracleStatementParser oracleParser = new OracleStatementParser(stmt.toString());
				  SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();
				  SQLSelectQuery oracleSqlSelectQuery = oracleStmt.getSelect().getQuery();
				  if(oracleSqlSelectQuery instanceof OracleSelectQueryBlock)
				  {
					  parseOraclePageSql(oracleStmt,rrs, (OracleSelectQueryBlock) oracleSqlSelectQuery,schema);
				  }



			  }
			if(isNeedParseOrderAgg)
			{
				parseOrderAggGroupMysql(rrs, mysqlSelectQuery);
				//更改canRunInReadDB属性
				if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false)
				{
					rrs.setCanRunInReadDB(false);
				}
			}

			//更改canRunInReadDB属性
			if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false) {
				rrs.setCanRunInReadDB(false);
			}

		} else if (sqlSelectQuery instanceof MySqlUnionQuery) { //TODO union语句可能需要额外考虑，目前不处理也没问题
//			MySqlUnionQuery unionQuery = (MySqlUnionQuery)sqlSelectQuery;
//			MySqlSelectQueryBlock left = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			MySqlSelectQueryBlock right = (MySqlSelectQueryBlock)unionQuery.getLeft();
//			System.out.println();
		}
	}


	private void parseOrderAggGroupOracle(RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery)
	{
		Map<String, String> aliaColumns = parseAggGroupCommon(rrs, mysqlSelectQuery);

		OracleSelect oracleSelect= (OracleSelect) mysqlSelectQuery.getParent();
		if(oracleSelect.getOrderBy() != null) {
			List<SQLSelectOrderByItem> orderByItems = oracleSelect.getOrderBy().getItems();
			rrs.setOrderByCols(buildOrderByCols(orderByItems,aliaColumns));
		}
	}


	private void parseOraclePageSql(SQLStatement stmt,RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery,SchemaConfig schema)
	{
		//第一层子查询
		SQLExpr where=  mysqlSelectQuery.getWhere();
		SQLTableSource from= mysqlSelectQuery.getFrom();
		if(where instanceof SQLBinaryOpExpr &&from instanceof SQLSubqueryTableSource)
        {

            SQLBinaryOpExpr one= (SQLBinaryOpExpr) where;
            String left=one.getLeft().toString();
            SQLBinaryOperator operator =one.getOperator();

			if(one.getRight() instanceof SQLIntegerExpr &&"rownum".equalsIgnoreCase(left)
					&&(operator==SQLBinaryOperator.LessThanOrEqual||operator==SQLBinaryOperator.LessThan))
			{
				SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
				int firstrownum = right.getNumber().intValue();
				if (operator == SQLBinaryOperator.LessThan) firstrownum = firstrownum - 1;
				SQLSelectQuery subSelect = ((SQLSubqueryTableSource) from).getSelect().getQuery();
				if (subSelect instanceof OracleSelectQueryBlock)
				{
					rrs.setLimitStart(0);
					rrs.setLimitSize(firstrownum);
					mysqlSelectQuery = (OracleSelectQueryBlock) subSelect;    //为了继续解出order by 等
					parseOrderAggGroupOracle(rrs, mysqlSelectQuery);
					isNeedParseOrderAgg=false;
				}
			}
			else
            if(one.getRight() instanceof SQLIntegerExpr &&!"rownum".equalsIgnoreCase(left)
                    &&(operator==SQLBinaryOperator.GreaterThan||operator==SQLBinaryOperator.GreaterThanOrEqual))
           {
			   SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
			   int firstrownum = right.getNumber().intValue();
			   if (operator == SQLBinaryOperator.GreaterThanOrEqual) firstrownum = firstrownum - 1;
				   SQLSelectQuery subSelect = ((SQLSubqueryTableSource) from).getSelect().getQuery();
				   if (subSelect instanceof OracleSelectQueryBlock)
				   {  //第二层子查询
					   OracleSelectQueryBlock twoSubSelect = (OracleSelectQueryBlock) subSelect;
					   if (twoSubSelect.getWhere() instanceof SQLBinaryOpExpr && twoSubSelect.getFrom() instanceof SQLSubqueryTableSource)
					   {
						   SQLBinaryOpExpr twoWhere = (SQLBinaryOpExpr) twoSubSelect.getWhere();
						   boolean isRowNum = "rownum".equalsIgnoreCase(twoWhere.getLeft().toString());
						   boolean isLess = twoWhere.getOperator() == SQLBinaryOperator.LessThanOrEqual || twoWhere.getOperator() == SQLBinaryOperator.LessThan;
						   if (isRowNum && twoWhere.getRight() instanceof SQLIntegerExpr && isLess)
						   {
							   int lastrownum = ((SQLIntegerExpr) twoWhere.getRight()).getNumber().intValue();
							   if (operator == SQLBinaryOperator.LessThan) lastrownum = lastrownum - 1;
							   SQLSelectQuery finalQuery = ((SQLSubqueryTableSource) twoSubSelect.getFrom()).getSelect().getQuery();
							   if (finalQuery instanceof OracleSelectQueryBlock)
							   {
								   rrs.setLimitStart(firstrownum);
								   rrs.setLimitSize(lastrownum - firstrownum);
								   LayerCachePool tableId2DataNodeCache = (LayerCachePool) MycatServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
								   try
								   {
									   RouterUtil.tryRouteForTables(schema, getCtx(), rrs, true, tableId2DataNodeCache);
								   } catch (SQLNonTransientException e)
								   {
									   throw new RuntimeException(e);
								   }
								   if (isNeedChangeLimit(rrs, schema))
								   {
									   one.setRight(new SQLIntegerExpr(0));
									   rrs.changeNodeSqlAfterAddLimit(stmt.toString());
									   //设置改写后的sql
									   ctx.setSql(stmt.toString());
								   }
								   mysqlSelectQuery = (OracleSelectQueryBlock) finalQuery;    //为了继续解出order by 等
								   parseOrderAggGroupOracle(rrs, mysqlSelectQuery);
								   isNeedParseOrderAgg=false;
							   }

						   }

					   }

				   }
			   }

        }

	}



	protected String  convertToNativePageSql(String sql,int offset,int count)
	{
		OracleStatementParser oracleParser = new OracleStatementParser(sql);
		SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();

		return 	PagerUtils.limit(oracleStmt.getSelect(), "oracle", offset, count)  ;

	}
	

}
