package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGSQLStatementParser;

public class DruidSelectPostgresqlParser extends DruidSelectParser
{


    protected String convertToNativePageSql(String sql, int offset, int count)
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
        return PagerUtils.limit(select, "postgresql", offset, count);

    }


}
