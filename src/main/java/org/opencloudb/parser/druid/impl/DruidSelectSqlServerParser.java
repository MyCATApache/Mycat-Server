package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.db2.parser.DB2StatementParser;
import com.alibaba.druid.sql.dialect.sqlserver.parser.SQLServerStatementParser;

public class DruidSelectSqlServerParser extends DruidSelectParser {




	protected String  convertToNativePageSql(String sql,int offset,int count)
	{
		SQLServerStatementParser oracleParser = new SQLServerStatementParser(sql);
		SQLSelectStatement oracleStmt = (SQLSelectStatement) oracleParser.parseStatement();

		return 	PagerUtils.limit(oracleStmt.getSelect(), "sqlserver", offset, count)  ;

	}
	

}
