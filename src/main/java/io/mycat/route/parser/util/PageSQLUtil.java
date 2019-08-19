package io.mycat.route.parser.util;

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
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGSQLStatementParser;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelectQueryBlock;
import com.alibaba.druid.sql.dialect.sqlserver.parser.SQLServerStatementParser;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * SQL分页工具
 * Created by magicdoom on 2015/3/15.
 */
public class PageSQLUtil {

    /**
     * 将limit转成本地支持的数据库方言的分页sql
     * @param dbType
     * @param sql
     * @param offset
     * @param count
     * @return
     */
    public static String convertLimitToNativePageSql(String dbType, String sql, int offset, int count) {
        if (JdbcConstants.ORACLE.equalsIgnoreCase(dbType)) { // Oracle数据库
            OracleStatementParser oracleParser = new OracleStatementParser(sql);
            SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();
            // druid 新版将分页查询直接生产为以下方式，表示跳过5行取后10行，如果改为新的方式就需要做版本兼容，同时需要修改mycat相关的分页属性(limitStart，limitSize)
            // oracleStmt = SELECT * FROM offer ORDER BY id DESC OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY
            // return SQLUtils.toSQLString(oracleStmt.getSelect(), dbType);
            // 因为目前的Mycat使用的是旧的oracle分页方式(在Oracle 12c之前的版本)，所以需要将limit设为空，
            // 利用 PagerUtils.limit 方法生产旧的分页查询方式如下，ROWNUM是个伪列，是随着结果集生成的，返回的第一行分配的是1，第二行是2等等，生成的结果是依次递加的，没有1就不会有2
            // SELECT XX.*, ROWNUM AS RN FROM (SELECT * FROM offer ORDER BY id DESC ) XX WHERE ROWNUM <= 15
            ((OracleSelectQueryBlock)oracleStmt.getSelect().getQuery()).setLimit(null);
            return PagerUtils.limit(oracleStmt.getSelect(), JdbcConstants.ORACLE, offset, count);
        } else if (JdbcConstants.SQL_SERVER.equalsIgnoreCase(dbType)) { // SQLServer数据库
            SQLServerStatementParser sqlServerParser = new SQLServerStatementParser(sql);
            SQLSelectStatement sqlserverStmt = (SQLSelectStatement) sqlServerParser.parseStatement();
            SQLSelect select = sqlserverStmt.getSelect();
            SQLOrderBy orderBy = select.getOrderBy() ;
            if(orderBy == null) {
                SQLSelectQuery sqlSelectQuery = select.getQuery();
                if(sqlSelectQuery instanceof SQLServerSelectQueryBlock) {
                    SQLServerSelectQueryBlock sqlServerSelectQueryBlock = (SQLServerSelectQueryBlock) sqlSelectQuery;
                    SQLTableSource from = sqlServerSelectQueryBlock.getFrom();
                    if("limit".equalsIgnoreCase(from.getAlias())) {
                        from.setAlias(null);
                    }
                }
            }
            return 	PagerUtils.limit(select, JdbcConstants.SQL_SERVER, offset, count)  ;
        } else if (JdbcConstants.DB2.equalsIgnoreCase(dbType)) { // DB2数据库
            DB2StatementParser db2Parser = new DB2StatementParser(sql);
            SQLSelectStatement db2Stmt = (SQLSelectStatement) db2Parser.parseStatement();

            return limitDB2(db2Stmt.getSelect(), JdbcConstants.DB2, offset, count);
        }  else if (JdbcConstants.POSTGRESQL.equalsIgnoreCase(dbType)) { // PostgreSQL数据库
            PGSQLStatementParser pgParser = new PGSQLStatementParser(sql);
            SQLSelectStatement pgStmt = (SQLSelectStatement) pgParser.parseStatement();
            SQLSelect select = pgStmt.getSelect();
            SQLSelectQuery query= select.getQuery();
            if(query instanceof PGSelectQueryBlock) {
                PGSelectQueryBlock pgSelectQueryBlock= (PGSelectQueryBlock) query;
                pgSelectQueryBlock.setOffset(null);
                pgSelectQueryBlock.setLimit(null);

            }
            return PagerUtils.limit(select, JdbcConstants.POSTGRESQL, offset, count);

        }  else if (JdbcConstants.MYSQL.equalsIgnoreCase(dbType)) { // MySQL数据库
            MySqlStatementParser pgParser = new MySqlStatementParser(sql);
            SQLSelectStatement pgStmt = (SQLSelectStatement) pgParser.parseStatement();
            SQLSelect select = pgStmt.getSelect();
            SQLSelectQuery query= select.getQuery();
            if(query instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock pgSelectQueryBlock= (MySqlSelectQueryBlock) query;
                pgSelectQueryBlock.setLimit(null);
            }
            return PagerUtils.limit(select, JdbcConstants.MYSQL, offset, count);
        }

        return sql;

    }
    private static String limitDB2(SQLSelect select, String dbType, int offset, int count) {
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

        if (query instanceof SQLSelectQueryBlock) {
            DB2SelectQueryBlock queryBlock = (DB2SelectQueryBlock) query;

            List<SQLSelectItem> selectItemList = queryBlock.getSelectList();
            for (int i = 0; i < selectItemList.size(); i++) {
                SQLSelectItem sqlSelectItem = selectItemList.get(i);
                SQLExpr expr = sqlSelectItem.getExpr();
                String alias = sqlSelectItem.getAlias();
                if (expr instanceof SQLAllColumnExpr && alias == null) {
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

        if (offset <= 0) {
            return SQLUtils.toSQLString(countQueryBlock, dbType);
        }

        DB2SelectQueryBlock offsetQueryBlock = new DB2SelectQueryBlock();
        offsetQueryBlock.getSelectList().add(new SQLSelectItem(new SQLAllColumnExpr()));
        offsetQueryBlock.setFrom(new SQLSubqueryTableSource(new SQLSelect(countQueryBlock), "XXX"));
        offsetQueryBlock.setWhere(pageCondition);

        return SQLUtils.toSQLString(offsetQueryBlock, dbType);
    }

}
