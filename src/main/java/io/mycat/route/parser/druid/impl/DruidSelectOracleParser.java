package io.mycat.route.parser.druid.impl;

import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelect;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.util.JdbcConstants;

import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;

public class DruidSelectOracleParser extends DruidSelectParser {

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
					  parseNativePageSql(oracleStmt, rrs, (OracleSelectQueryBlock) oracleSqlSelectQuery, schema);
				  }



			  }
			if(isNeedParseOrderAgg)
			{
				parseOrderAggGroupMysql(schema, selectStmt,rrs, mysqlSelectQuery);
				//更改canRunInReadDB属性
				if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false)
				{
					rrs.setCanRunInReadDB(false);
				}
			}

		}


	}


	protected void parseOrderAggGroupOracle(SQLStatement stmt, RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery, SchemaConfig schema)
	{
		Map<String, String> aliaColumns = parseAggGroupCommon(schema, stmt,rrs, mysqlSelectQuery);

		OracleSelect oracleSelect= (OracleSelect) mysqlSelectQuery.getParent();
		if(oracleSelect.getOrderBy() != null) {
			List<SQLSelectOrderByItem> orderByItems = oracleSelect.getOrderBy().getItems();
			rrs.setOrderByCols(buildOrderByCols(orderByItems,aliaColumns));
		}
        isNeedParseOrderAgg=false;
	}


	protected void parseNativePageSql(SQLStatement stmt, RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery, SchemaConfig schema)
	{
		//第一层子查询
		SQLExpr where=  mysqlSelectQuery.getWhere();
		SQLTableSource from= mysqlSelectQuery.getFrom();
		if(where instanceof SQLBinaryOpExpr &&from instanceof SQLSubqueryTableSource)
        {

            SQLBinaryOpExpr one= (SQLBinaryOpExpr) where;
            SQLExpr left=one.getLeft();
            SQLBinaryOperator operator =one.getOperator();

              //解析只有一层rownum限制大小
			if(one.getRight() instanceof SQLIntegerExpr &&"rownum".equalsIgnoreCase(left.toString())
					&&(operator==SQLBinaryOperator.LessThanOrEqual||operator==SQLBinaryOperator.LessThan))
			{
				SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
				int firstrownum = right.getNumber().intValue();
				if (operator == SQLBinaryOperator.LessThan&&firstrownum!=0) {
					firstrownum = firstrownum - 1;
				}
				SQLSelectQuery subSelect = ((SQLSubqueryTableSource) from).getSelect().getQuery();
				if (subSelect instanceof OracleSelectQueryBlock)
				{
					rrs.setLimitStart(0);
					rrs.setLimitSize(firstrownum);
					mysqlSelectQuery = (OracleSelectQueryBlock) subSelect;    //为了继续解出order by 等
					parseOrderAggGroupOracle(stmt,rrs, mysqlSelectQuery, schema);
					isNeedParseOrderAgg=false;
				}
			}
			else //解析oracle三层嵌套分页
            if(one.getRight() instanceof SQLIntegerExpr &&!"rownum".equalsIgnoreCase(left.toString())
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
								if (operator == SQLBinaryOperator.LessThan&&firstrownum!=0) {
									firstrownum = firstrownum - 1;
								}
								if (subSelect instanceof OracleSelectQueryBlock)
								{
									rrs.setLimitStart(0);
									rrs.setLimitSize(firstrownum);
									mysqlSelectQuery = (OracleSelectQueryBlock) subSelect;
									if(orderBy!=null)
									{
										OracleSelect oracleSelect= (OracleSelect) subSelect.getParent();
										oracleSelect.setOrderBy(orderBy);
									}
									parseOrderAggGroupOracle(stmt,rrs, mysqlSelectQuery, schema);
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
								if(leftE.getOperator()==SQLBinaryOperator.GreaterThanOrEqual &&firstrownum!=0) {
									firstrownum = firstrownum - 1;
								}
							} else
							if(leftE.getRight() instanceof SQLIntegerExpr&&(leftE.getOperator()==SQLBinaryOperator.LessThan||leftE.getOperator()==SQLBinaryOperator.LessThanOrEqual))
							{
								larger=leftE;
								lastrownum=((SQLIntegerExpr) leftE.getRight()).getNumber().intValue();
								if(leftE.getOperator()==SQLBinaryOperator.LessThan&&lastrownum!=0) {
									lastrownum = lastrownum - 1;
								}
							}

							if(rightE.getRight() instanceof SQLIntegerExpr&&(rightE.getOperator()==SQLBinaryOperator.GreaterThan||rightE.getOperator()==SQLBinaryOperator.GreaterThanOrEqual))
							{
								small=rightE;
								firstrownum=((SQLIntegerExpr) rightE.getRight()).getNumber().intValue();
								if(rightE.getOperator()==SQLBinaryOperator.GreaterThanOrEqual&&firstrownum!=0) {
									firstrownum = firstrownum - 1;
								}
							} else
							if(rightE.getRight() instanceof SQLIntegerExpr&&(rightE.getOperator()==SQLBinaryOperator.LessThan||rightE.getOperator()==SQLBinaryOperator.LessThanOrEqual))
							{
								larger=rightE;
								lastrownum=((SQLIntegerExpr) rightE.getRight()).getNumber().intValue();
								if(rightE.getOperator()==SQLBinaryOperator.LessThan&&lastrownum!=0) {
									lastrownum = lastrownum - 1;
								}
							}
							if(small!=null&&larger!=null)
							{
								setLimitIFChange(stmt, rrs, schema, small, firstrownum, lastrownum);
								if(orderBy!=null)
								{
									OracleSelect oracleSelect= (OracleSelect) subSelect.getParent();
									oracleSelect.setOrderBy(orderBy);
								}
								parseOrderAggGroupOracle(stmt,rrs, (OracleSelectQueryBlock) subSelect, schema);
								isNeedParseOrderAgg=false;
							}

                        }


                    } else
                        {
                            parseNativeSql(stmt,rrs,mysqlSelectQuery,schema);
                        }



                }

        }

		}
        else
        {
            parseNativeSql(stmt,rrs,mysqlSelectQuery,schema);
        }
        if(isNeedParseOrderAgg)
        {
            parseOrderAggGroupOracle(stmt,rrs,  mysqlSelectQuery, schema);
        }
    }

	protected String getCurentDbType()
	{
		return JdbcConstants.ORACLE;
	}
    protected void parseNativeSql(SQLStatement stmt,RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery,SchemaConfig schema)
    {
		 //解析分页以外的语法
    }

	private void parseThreeLevelPageSql(SQLStatement stmt, RouteResultset rrs, SchemaConfig schema, SQLSubqueryTableSource from, SQLBinaryOpExpr one, SQLBinaryOperator operator)
	{
        SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
		int firstrownum = right.getNumber().intValue();
		if (operator == SQLBinaryOperator.GreaterThanOrEqual&&firstrownum!=0) {
			firstrownum = firstrownum - 1;
		}
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
                    if (operator == SQLBinaryOperator.LessThan&&lastrownum!=0) {
						lastrownum = lastrownum - 1;
					}
                    SQLSelectQuery finalQuery = ((SQLSubqueryTableSource) twoSubSelect.getFrom()).getSelect().getQuery();
                    if (finalQuery instanceof OracleSelectQueryBlock)
                    {
						setLimitIFChange(stmt, rrs, schema, one, firstrownum, lastrownum);
                        parseOrderAggGroupOracle(stmt,rrs, (OracleSelectQueryBlock) finalQuery, schema);
                        isNeedParseOrderAgg=false;
                    }

                }

            }

        }
	}





	

}
