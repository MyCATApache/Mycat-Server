package org.opencloudb.parser.util;

import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import com.alibaba.druid.sql.dialect.db2.parser.DB2StatementParser;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGSQLStatementParser;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelectQueryBlock;
import com.alibaba.druid.sql.dialect.sqlserver.parser.SQLServerStatementParser;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * Created by magicdoom on 2015/3/15.
 */
public class PageSQLUtil
{
    public static String convertLimitToNativePageSql(String dbType, String sql, int offset, int count)
    {
        if (JdbcConstants.ORACLE.equalsIgnoreCase(dbType))
        {
            OracleStatementParser oracleParser = new OracleStatementParser(sql);
            SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();

            return PagerUtils.limit(oracleStmt.getSelect(), JdbcConstants.ORACLE, offset, count);
        } else if (JdbcConstants.SQL_SERVER.equalsIgnoreCase(dbType))
        {
            SQLServerStatementParser oracleParser = new SQLServerStatementParser(sql);
            SQLSelectStatement sqlserverStmt = (SQLSelectStatement) oracleParser.parseStatement();
            SQLSelect select = sqlserverStmt.getSelect();
            SQLOrderBy orderBy=  select.getOrderBy() ;
            if(orderBy==null)
            {
                SQLSelectQuery sqlSelectQuery=      select.getQuery();
                if(sqlSelectQuery instanceof SQLServerSelectQueryBlock)
                {
                    SQLServerSelectQueryBlock sqlServerSelectQueryBlock= (SQLServerSelectQueryBlock) sqlSelectQuery;
                    SQLTableSource from=       sqlServerSelectQueryBlock.getFrom();
                    if("limit".equalsIgnoreCase(from.getAlias()))
                    {
                        from.setAlias(null);
                    }
                }
                SQLOrderBy newOrderBy=new SQLOrderBy(new SQLIdentifierExpr("(select 0)"));
                select.setOrderBy(newOrderBy);

            }

            return 	PagerUtils.limit(select, JdbcConstants.SQL_SERVER, offset, count)  ;
        }
        else if (JdbcConstants.DB2.equalsIgnoreCase(dbType))
        {
            DB2StatementParser db2Parser = new DB2StatementParser(sql);
            SQLSelectStatement db2Stmt = (SQLSelectStatement) db2Parser.parseStatement();

            return limitDB2(db2Stmt.getSelect(), JdbcConstants.DB2, offset, count);
        }  else if (JdbcConstants.POSTGRESQL.equalsIgnoreCase(dbType))
        {
            PGSQLStatementParser pgParser = new PGSQLStatementParser(sql);
            SQLSelectStatement pgStmt = (SQLSelectStatement) pgParser.parseStatement();
            SQLSelect select = pgStmt.getSelect();
            SQLSelectQuery query= select.getQuery();
            if(query instanceof PGSelectQueryBlock)
            {
                PGSelectQueryBlock pgSelectQueryBlock= (PGSelectQueryBlock) query;
                pgSelectQueryBlock.setLimit(null);
                pgSelectQueryBlock.setOffset(null);
            }
            return PagerUtils.limit(select, JdbcConstants.POSTGRESQL, offset, count);

        }  else if (JdbcConstants.MYSQL.equalsIgnoreCase(dbType))
        {
            MySqlStatementParser pgParser = new MySqlStatementParser(sql);
            SQLSelectStatement pgStmt = (SQLSelectStatement) pgParser.parseStatement();
            SQLSelect select = pgStmt.getSelect();
            SQLSelectQuery query= select.getQuery();
            if(query instanceof MySqlSelectQueryBlock)
            {
                MySqlSelectQueryBlock pgSelectQueryBlock= (MySqlSelectQueryBlock) query;
                pgSelectQueryBlock.setLimit(null);
            }
            return PagerUtils.limit(select, JdbcConstants.MYSQL, offset, count);
        }

        return sql;

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
