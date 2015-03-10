package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGSQLStatementParser;

public class DruidSelectPostgresqlParser extends DruidSelectParser
{


    protected String convertToNativePageSql(String sql, int offset, int count)
    {
        PGSQLStatementParser oracleParser = new PGSQLStatementParser(sql);
        SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();

        return PagerUtils.limit(oracleStmt.getSelect(), "postgresql", offset, count);

    }


}
