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
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelect;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelectQueryBlock;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerTop;
import com.alibaba.druid.sql.dialect.sqlserver.parser.SQLServerStatementParser;
import com.alibaba.druid.util.JdbcConstants;

import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;

public class DruidSelectSqlServerParser extends DruidSelectParser {

	public DruidSelectSqlServerParser(){
		super();
		isNeedParseOrderAgg=true;
	}

	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) {
		SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		//从mysql解析过来
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			MySqlSelectQueryBlock.Limit limit=mysqlSelectQuery.getLimit();
			if(limit==null)
			{
                sqlserverParse(schema, rrs);


            }
			if(isNeedParseOrderAgg)
			{
				parseOrderAggGroupMysql(schema, stmt,rrs, mysqlSelectQuery);
				//更改canRunInReadDB属性
				if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false)
				{
					rrs.setCanRunInReadDB(false);
				}
			}

		}


	}
	protected String getCurentDbType()
	{
		return JdbcConstants.SQL_SERVER;
	}
    private void sqlserverParse(SchemaConfig schema, RouteResultset rrs)
    {
        //使用sqlserver的解析，否则会有部分语法识别错误
        SQLServerStatementParser oracleParser = new SQLServerStatementParser(getCtx().getSql());
        SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();
        SQLSelectQuery oracleSqlSelectQuery = oracleStmt.getSelect().getQuery();
        if(oracleSqlSelectQuery instanceof SQLServerSelectQueryBlock)
        {
            parseSqlServerPageSql(oracleStmt, rrs, (SQLServerSelectQueryBlock) oracleSqlSelectQuery, schema);
            if(isNeedParseOrderAgg)
            {
                parseOrderAggGroupSqlServer(schema, oracleStmt,rrs, (SQLServerSelectQueryBlock) oracleSqlSelectQuery);
            }
        }

    }


    private void parseOrderAggGroupSqlServer(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs, SQLServerSelectQueryBlock mysqlSelectQuery)
	{
		Map<String, String> aliaColumns = parseAggGroupCommon(schema, stmt,rrs, mysqlSelectQuery);

		SQLServerSelect oracleSelect= (SQLServerSelect) mysqlSelectQuery.getParent();
		if(oracleSelect.getOrderBy() != null) {
			List<SQLSelectOrderByItem> orderByItems = oracleSelect.getOrderBy().getItems();
			rrs.setOrderByCols(buildOrderByCols(orderByItems,aliaColumns));
		}
	}

	private void parseSqlServerPageSql(SQLStatement stmt, RouteResultset rrs, SQLServerSelectQueryBlock sqlserverSelectQuery, SchemaConfig schema)
	{
		//第一层子查询
		SQLExpr where=  sqlserverSelectQuery.getWhere();
		SQLTableSource from= sqlserverSelectQuery.getFrom();
        if(sqlserverSelectQuery.getTop()!=null)
        {
            SQLServerTop top= sqlserverSelectQuery.getTop() ;
            SQLExpr sqlExpr=  top.getExpr()  ;
            if(sqlExpr instanceof SQLIntegerExpr)
            {

                int    topValue=((SQLIntegerExpr) sqlExpr).getNumber().intValue();
                rrs.setLimitStart(0);
                rrs.setLimitSize(topValue);
            }
        }
        else
		if(where instanceof SQLBinaryOpExpr &&from instanceof SQLSubqueryTableSource)
		{

			SQLBinaryOpExpr one= (SQLBinaryOpExpr) where;
			SQLExpr left=one.getLeft();
			SQLBinaryOperator operator =one.getOperator();
			SQLSelectQuery subSelect = ((SQLSubqueryTableSource) from).getSelect().getQuery();
			SQLOrderBy orderBy=null;
			if (subSelect instanceof SQLServerSelectQueryBlock)
			{
				boolean hasRowNumber=false;
                boolean hasSubTop=false;
                int subTop=0;
				SQLServerSelectQueryBlock subSelectOracle = (SQLServerSelectQueryBlock) subSelect;
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
                if(subSelectOracle.getFrom() instanceof SQLSubqueryTableSource)
                {
                    SQLSubqueryTableSource subFrom= (SQLSubqueryTableSource) subSelectOracle.getFrom();
                    if (subFrom.getSelect().getQuery() instanceof SQLServerSelectQueryBlock)
                    {
                        SQLServerSelectQueryBlock sqlSelectQuery = (SQLServerSelectQueryBlock) subFrom.getSelect().getQuery();
                        if(sqlSelectQuery.getTop()!=null)
                        {

                            SQLExpr sqlExpr=  sqlSelectQuery.getTop().getExpr()  ;
                            if(sqlExpr instanceof SQLIntegerExpr)
                            {
                                hasSubTop=true;
                                subTop=((SQLIntegerExpr) sqlExpr).getNumber().intValue();
                                orderBy=  subFrom.getSelect().getOrderBy();
                            }
                        }

                    }
                }

				if(hasRowNumber)
				{
                     if(hasSubTop&&(operator==SQLBinaryOperator.GreaterThan||operator==SQLBinaryOperator.GreaterThanOrEqual)&& one.getRight() instanceof SQLIntegerExpr)
                     {
                         SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
                         int firstrownum = right.getNumber().intValue();
                         if (operator == SQLBinaryOperator.GreaterThanOrEqual&&firstrownum!=0) {
							 firstrownum = firstrownum - 1;
						 }
                         int lastrownum =subTop;
                         setLimitIFChange(stmt, rrs, schema, one, firstrownum, lastrownum);
                         if(orderBy!=null)
                         {
                             SQLServerSelect oracleSelect= (SQLServerSelect) subSelect.getParent();
                             oracleSelect.setOrderBy(orderBy);
                         }
                         parseOrderAggGroupSqlServer(schema, stmt,rrs, (SQLServerSelectQueryBlock) subSelect);
                         isNeedParseOrderAgg=false;

                     }
                       else

					if((operator==SQLBinaryOperator.LessThan||operator==SQLBinaryOperator.LessThanOrEqual) && one.getRight() instanceof SQLIntegerExpr )
					{
						SQLIntegerExpr right = (SQLIntegerExpr) one.getRight();
						int firstrownum = right.getNumber().intValue();
						if (operator == SQLBinaryOperator.LessThan&&firstrownum!=0) {
							firstrownum = firstrownum - 1;
						}
						if (subSelect instanceof SQLServerSelectQueryBlock)
						{
							rrs.setLimitStart(0);
							rrs.setLimitSize(firstrownum);
							sqlserverSelectQuery = (SQLServerSelectQueryBlock) subSelect;    //为了继续解出order by 等
							if(orderBy!=null)
							{
								SQLServerSelect oracleSelect= (SQLServerSelect) subSelect.getParent();
								oracleSelect.setOrderBy(orderBy);
							}
							parseOrderAggGroupSqlServer(schema, stmt,rrs, sqlserverSelectQuery);
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
								SQLServerSelect oracleSelect= (SQLServerSelect) subSelect.getParent();
								oracleSelect.setOrderBy(orderBy);
							}
							parseOrderAggGroupSqlServer(schema, stmt,rrs, (SQLServerSelectQueryBlock) subSelect);
							isNeedParseOrderAgg=false;
						}

					}


				}



			}

		}

	}


	

}
