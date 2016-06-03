package io.mycat.route.parser.druid.impl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelect;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;

import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;


/**
 * 由于druid的db2解析部分不够完整，且使用oracle的解析基本能满足需求
 * 所以基于oracle的扩展
 */
public class DruidSelectDb2Parser extends DruidSelectOracleParser
{

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
                                    mysqlSelectQuery = (OracleSelectQueryBlock) subSelect;    //为了继续解出order by 等
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
        else
        {
            parseNativeSql(stmt,rrs,mysqlSelectQuery,schema);
        }
        if(isNeedParseOrderAgg)
        {
            parseOrderAggGroupOracle(stmt,rrs,  mysqlSelectQuery, schema);
        }
    }


    private static final  Pattern pattern = Pattern.compile("FETCH(?:\\s)+FIRST(?:\\s)+(\\d+)(?:\\s)+ROWS(?:\\s)+ONLY",Pattern.CASE_INSENSITIVE);
    protected void parseNativeSql(SQLStatement stmt,RouteResultset rrs, OracleSelectQueryBlock mysqlSelectQuery,SchemaConfig schema)
    {

        Matcher matcher = pattern.matcher(getCtx().getSql());
        while (matcher.find())
        {

          String  row=    matcher.group(1);
            rrs.setLimitStart(0);
            rrs.setLimitSize(Integer.parseInt(row));
        }
    }

    protected String getCurentDbType()
    {
        return JdbcConstants.DB2;
    }




}
