package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;


import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.util.StringUtil;

/**
 * alter table 语句解析
 * @author wang.dw
 *
 */
public class DruidAlterTableParser extends DefaultDruidParser {
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt,MycatSchemaStatVisitor visitor) throws SQLNonTransientException {
		
	}
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement)stmt;
	String tableName = StringUtil.removeBackquote(alterTable.getTableSource().toString().toUpperCase());
//
	ctx.addTable(tableName);
		
	}

//    public static void main(String[] args)
//    {
//        String s="SELECT Customer,SUM(OrderPrice) FROM Orders\n" +
//                "GROUP BY Customer";
//        SQLStatementParser parser = new MySqlStatementParser(s);
//        SQLStatement statement = parser.parseStatement();
//        System.out.println();
//    }
}
