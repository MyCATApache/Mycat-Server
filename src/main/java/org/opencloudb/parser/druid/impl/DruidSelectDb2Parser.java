package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import com.alibaba.druid.sql.dialect.db2.parser.DB2StatementParser;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelect;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.route.RouteResultset;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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



        }
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
        String patten="FETCH(?:\\s)+FIRST(?:\\s)+(\\d+)(?:\\s)+ROWS(?:\\s)+ONLY";
        Pattern pattern = Pattern.compile(patten,Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(getCtx().getSql());
        while (matcher.find())
        {

          String  row=    matcher.group(1);
            rrs.setLimitStart(0);
            rrs.setLimitSize(Integer.parseInt(row));
        }
    }


    protected String convertLimitToNativePageSql(SQLStatement stmt, String sql, int offset, int count)
    {
        DB2StatementParser db2Parser = new DB2StatementParser(sql);
        SQLSelectStatement db2Stmt = (SQLSelectStatement) db2Parser.parseStatement();

        return limitDB2(db2Stmt.getSelect(), "db2", offset, count);

    }

    private static String limitDB2(SQLSelect select, String dbType, int offset, int count)
    {
        SQLSelectQuery query = select.getQuery();

        SQLBinaryOpExpr gt = new SQLBinaryOpExpr(new SQLIdentifierExpr("ROWNUM"), //
                SQLBinaryOperator.GreaterThan, //
                new SQLNumberExpr(offset), //
                JdbcConstants.DB2);
        SQLBinaryOpExpr lteq = new SQLBinaryOpExpr(new SQLIdentifierExpr("ROWNUM"), //
                SQLBinaryOperator.LessThanOrEqual, //
                new SQLNumberExpr(count + offset), //
                JdbcConstants.DB2);
        SQLBinaryOpExpr pageCondition = new SQLBinaryOpExpr(gt, SQLBinaryOperator.BooleanAnd, lteq, JdbcConstants.DB2);

        if (query instanceof SQLSelectQueryBlock)
        {
            DB2SelectQueryBlock queryBlock = (DB2SelectQueryBlock) query;

            List<SQLSelectItem> selectItemList = queryBlock.getSelectList();
            for (int i = 0; i < selectItemList.size(); i++)
            {
                SQLSelectItem sqlSelectItem = selectItemList.get(i);
                SQLExpr expr = sqlSelectItem.getExpr();
                String alias = sqlSelectItem.getAlias();
                if (expr instanceof SQLAllColumnExpr && alias == null)
                {
                    //未加别名会报语法错误
                    sqlSelectItem.setExpr(new SQLPropertyExpr(new SQLIdentifierExpr("XXYY"), "*"));
                    queryBlock.getFrom().setAlias("XXYY");
                }
            }

//      此处生成order by的顺序不对
//   if (offset <= 0) {
//                queryBlock.setFirst(new SQLNumberExpr(count));
//                return SQLUtils.toSQLString(select, dbType);
//            }

            SQLAggregateExpr aggregateExpr = new SQLAggregateExpr("ROW_NUMBER");
            SQLOrderBy orderBy = select.getOrderBy();
            aggregateExpr.setOver(new SQLOver(orderBy));
            select.setOrderBy(null);

            queryBlock.getSelectList().add(new SQLSelectItem(aggregateExpr, "ROWNUM"));

            DB2SelectQueryBlock countQueryBlock = new DB2SelectQueryBlock();
            countQueryBlock.getSelectList().add(new SQLSelectItem(new SQLAllColumnExpr()));

            countQueryBlock.setFrom(new SQLSubqueryTableSource(select, "XX"));

            countQueryBlock.setWhere(pageCondition);

            return SQLUtils.toSQLString(countQueryBlock, dbType);
        }

        DB2SelectQueryBlock countQueryBlock = new DB2SelectQueryBlock();
        countQueryBlock.getSelectList().add(new SQLSelectItem(new SQLPropertyExpr(new SQLIdentifierExpr("XX"), "*")));
        SQLAggregateExpr aggregateExpr = new SQLAggregateExpr("ROW_NUMBER");
        SQLOrderBy orderBy = select.getOrderBy();
        aggregateExpr.setOver(new SQLOver(orderBy));
        select.setOrderBy(null);
        countQueryBlock.getSelectList().add(new SQLSelectItem(aggregateExpr, "ROWNUM"));

        countQueryBlock.setFrom(new SQLSubqueryTableSource(select, "XX"));

        if (offset <= 0)
        {
            return SQLUtils.toSQLString(countQueryBlock, dbType);
        }

        DB2SelectQueryBlock offsetQueryBlock = new DB2SelectQueryBlock();
        offsetQueryBlock.getSelectList().add(new SQLSelectItem(new SQLAllColumnExpr()));
        offsetQueryBlock.setFrom(new SQLSubqueryTableSource(new SQLSelect(countQueryBlock), "XXX"));
        offsetQueryBlock.setWhere(pageCondition);

        return SQLUtils.toSQLString(offsetQueryBlock, dbType);
    }

}
