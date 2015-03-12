package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.ast.*;
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
       //从mysql解析过来
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			Limit limit=mysqlSelectQuery.getLimit();
			if(limit==null)
			{
				  //使用oracle的解析，否则会有部分oracle语法识别错误
				  OracleStatementParser oracleParser = new OracleStatementParser(getCtx().getSql());
				  SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();
                selectStmt= oracleStmt;
				  SQLSelectQuery oracleSqlSelectQuery = oracleStmt.getSelect().getQuery();
				  if(oracleSqlSelectQuery instanceof OracleSelectQueryBlock)
				  {
					  parseOraclePageSql(oracleStmt,rrs, (OracleSelectQueryBlock) oracleSqlSelectQuery,schema);
				  }



			  }
			if(isNeedParseOrderAgg)
			{
				parseOrderAggGroupMysql(selectStmt,rrs, mysqlSelectQuery);
				//更改canRunInReadDB属性
				if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false)
				{
					rrs.setCanRunInReadDB(false);
				}
			}

		}

          //从oracle解析过来   ,mysql解析出错才会到此 ,如rownumber分页
        else if (sqlSelectQuery instanceof OracleSelectQueryBlock) {

         parseOraclePageSql(stmt,rrs, (OracleSelectQueryBlock) sqlSelectQuery, schema);

		}
	}


	private void parseOrderAggGroupOracle(SQLStatement stmt,RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery)
	{
		Map<String, String> aliaColumns = parseAggGroupCommon(stmt,rrs, mysqlSelectQuery);

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
            SQLExpr left=one.getLeft();
            SQLBinaryOperator operator =one.getOperator();
            boolean isOracleDB=true; //由于db2的row_number解析与oracle相同，所以这里区分下
            List<String> tables= getCtx().getTables();
            for (String table : tables)
            {
               if( !schema.getTables().get(table).getDbTypes().contains("oracle") )
               {
                   isOracleDB=false;
                   break;
               }
            }

              //解析只有一层rownum限制大小
			if(isOracleDB&&one.getRight() instanceof SQLIntegerExpr &&"rownum".equalsIgnoreCase(left.toString())
					&&(operator==SQLBinaryOperator.LessThanOrEqual||operator==SQLBinaryOperator.LessThan))
			{
				SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
				int firstrownum = right.getNumber().intValue();
				if (operator == SQLBinaryOperator.LessThan&&firstrownum!=0) firstrownum = firstrownum - 1;
				SQLSelectQuery subSelect = ((SQLSubqueryTableSource) from).getSelect().getQuery();
				if (subSelect instanceof OracleSelectQueryBlock)
				{
					rrs.setLimitStart(0);
					rrs.setLimitSize(firstrownum);
					mysqlSelectQuery = (OracleSelectQueryBlock) subSelect;    //为了继续解出order by 等
					parseOrderAggGroupOracle(stmt,rrs, mysqlSelectQuery);
					isNeedParseOrderAgg=false;
				}
			}
			else //解析oracle三层嵌套分页
            if(isOracleDB&&one.getRight() instanceof SQLIntegerExpr &&!"rownum".equalsIgnoreCase(left.toString())
                    &&(operator==SQLBinaryOperator.GreaterThan||operator==SQLBinaryOperator.GreaterThanOrEqual))
           {
			   parseThreeLevelPageSql(stmt, rrs, schema, (SQLSubqueryTableSource) from, one, operator);
			   }
            else //解析oracle rownumber over分页
			{

                    SQLSelectQuery subSelect = ((SQLSubqueryTableSource) from).getSelect().getQuery();
					SQLOrderBy orderBy=null;
                    if (subSelect instanceof OracleSelectQueryBlock)
                    {
                        boolean hasRowNumber=false;
                        OracleSelectQueryBlock subSelectOracle = (OracleSelectQueryBlock) subSelect;
                        List<SQLSelectItem> sqlSelectItems=    subSelectOracle.getSelectList();
                        for (SQLSelectItem sqlSelectItem : sqlSelectItems)
                        {
                            SQLExpr sqlExpr=  sqlSelectItem.getExpr()   ;
                            if(sqlExpr instanceof  SQLAggregateExpr )
                            {
                                SQLAggregateExpr agg= (SQLAggregateExpr) sqlExpr;
                                if("row_number".equalsIgnoreCase(agg.getMethodName())&&agg.getOver()!=null)
                                {
                                    hasRowNumber=true;
                                     orderBy= agg.getOver().getOrderBy();
                                }

                            }
                        }

                        if(hasRowNumber)
                        {
							if((operator==SQLBinaryOperator.LessThan||operator==SQLBinaryOperator.LessThanOrEqual) && one.getRight() instanceof SQLIntegerExpr )
							{
								SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
								int firstrownum = right.getNumber().intValue();
								if (operator == SQLBinaryOperator.LessThan&&firstrownum!=0) firstrownum = firstrownum - 1;
								if (subSelect instanceof OracleSelectQueryBlock)
								{
									rrs.setLimitStart(0);
									rrs.setLimitSize(firstrownum);
									mysqlSelectQuery = (OracleSelectQueryBlock) subSelect;    //为了继续解出order by 等
									if(orderBy!=null)
									{
										OracleSelect oracleSelect= (OracleSelect) subSelect.getParent();
										oracleSelect.setOrderBy(orderBy);
									}
									parseOrderAggGroupOracle(stmt,rrs, mysqlSelectQuery);
									isNeedParseOrderAgg=false;
								}
							}
							  else
							if(operator==SQLBinaryOperator.BooleanAnd && left instanceof SQLBinaryOpExpr&&one.getRight() instanceof SQLBinaryOpExpr )
							{
							SQLBinaryOpExpr leftE= (SQLBinaryOpExpr) left;
							SQLBinaryOpExpr rightE= (SQLBinaryOpExpr) one.getRight();
							SQLBinaryOpExpr small=null ;
							SQLBinaryOpExpr larger=null ;
							int firstrownum =0;
							int lastrownum =0;
							if(leftE.getRight() instanceof SQLIntegerExpr&&(leftE.getOperator()==SQLBinaryOperator.GreaterThan||leftE.getOperator()==SQLBinaryOperator.GreaterThanOrEqual))
							{
								small=leftE;
								firstrownum=((SQLIntegerExpr) leftE.getRight()).getNumber().intValue();
								if(leftE.getOperator()==SQLBinaryOperator.GreaterThanOrEqual &&firstrownum!=0) firstrownum = firstrownum - 1;
							} else
							if(leftE.getRight() instanceof SQLIntegerExpr&&(leftE.getOperator()==SQLBinaryOperator.LessThan||leftE.getOperator()==SQLBinaryOperator.LessThanOrEqual))
							{
								larger=leftE;
								lastrownum=((SQLIntegerExpr) leftE.getRight()).getNumber().intValue();
								if(leftE.getOperator()==SQLBinaryOperator.LessThan&&lastrownum!=0) lastrownum = lastrownum - 1;
							}

							if(rightE.getRight() instanceof SQLIntegerExpr&&(rightE.getOperator()==SQLBinaryOperator.GreaterThan||rightE.getOperator()==SQLBinaryOperator.GreaterThanOrEqual))
							{
								small=rightE;
								firstrownum=((SQLIntegerExpr) rightE.getRight()).getNumber().intValue();
								if(rightE.getOperator()==SQLBinaryOperator.GreaterThanOrEqual&&firstrownum!=0) firstrownum = firstrownum - 1;
							} else
							if(rightE.getRight() instanceof SQLIntegerExpr&&(rightE.getOperator()==SQLBinaryOperator.LessThan||rightE.getOperator()==SQLBinaryOperator.LessThanOrEqual))
							{
								larger=rightE;
								lastrownum=((SQLIntegerExpr) rightE.getRight()).getNumber().intValue();
								if(rightE.getOperator()==SQLBinaryOperator.LessThan&&lastrownum!=0) lastrownum = lastrownum - 1;
							}
							if(small!=null&&larger!=null)
							{
								setLimitIFChange(stmt, rrs, schema, small, firstrownum, lastrownum);
								if(orderBy!=null)
								{
									OracleSelect oracleSelect= (OracleSelect) subSelect.getParent();
									oracleSelect.setOrderBy(orderBy);
								}
								parseOrderAggGroupOracle(stmt,rrs, (OracleSelectQueryBlock) subSelect);
								isNeedParseOrderAgg=false;
							}

                        }


                    } else
                        {
                            parseNativeSql(stmt,rrs,mysqlSelectQuery,schema);
                        }



                }

        }  }
        else
        {
            parseNativeSql(stmt,rrs,mysqlSelectQuery,schema);
        }
        if(isNeedParseOrderAgg)
        {
            parseOrderAggGroupOracle(stmt,rrs,  mysqlSelectQuery);
        }
    }


    protected void parseNativeSql(SQLStatement stmt,RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery,SchemaConfig schema)
    {

    }

	private void parseThreeLevelPageSql(SQLStatement stmt, RouteResultset rrs, SchemaConfig schema, SQLSubqueryTableSource from, SQLBinaryOpExpr one, SQLBinaryOperator operator)
	{
        SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
		int firstrownum = right.getNumber().intValue();
		if (operator == SQLBinaryOperator.GreaterThanOrEqual&&firstrownum!=0) firstrownum = firstrownum - 1;
		SQLSelectQuery subSelect = from.getSelect().getQuery();
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
                    if (operator == SQLBinaryOperator.LessThan&&lastrownum!=0) lastrownum = lastrownum - 1;
                    SQLSelectQuery finalQuery = ((SQLSubqueryTableSource) twoSubSelect.getFrom()).getSelect().getQuery();
                    if (finalQuery instanceof OracleSelectQueryBlock)
                    {
						setLimitIFChange(stmt, rrs, schema, one, firstrownum, lastrownum);
                        parseOrderAggGroupOracle(stmt,rrs, (OracleSelectQueryBlock) finalQuery);
                        isNeedParseOrderAgg=false;
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
