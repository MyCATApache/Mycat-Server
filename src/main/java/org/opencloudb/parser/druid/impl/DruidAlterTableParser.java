package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.route.RouteResultset;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement;

/**
 * alter table 语句解析
 * @author wang.dw
 *
 */
public class DruidAlterTableParser extends DefaultDruidParser {
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt,SchemaStatVisitor visitor) throws SQLNonTransientException {
		
	}
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlAlterTableStatement alterTable = (MySqlAlterTableStatement)stmt;
		String tableName = removeBackquote(alterTable.getTableSource().toString().toUpperCase());
		
		ctx.addTable(tableName);
		
	}
}
