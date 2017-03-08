package io.mycat.route.parser.druid.impl;

import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.util.StringUtil;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

public class DruidCreateTableParser extends DefaultDruidParser {

	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) {
	}
	
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement)stmt;
		if(createStmt.getQuery() != null) {
			String msg = "create table from other table not supported :" + stmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		String tableName = StringUtil.removeBackquote(createStmt.getTableSource().toString().toUpperCase());
		ctx.addTable(tableName);
		
	}
}
