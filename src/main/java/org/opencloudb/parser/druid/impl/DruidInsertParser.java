package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

public class DruidInsertParser extends DefaultDruidParser {
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		
	}
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlInsertStatement insert = (MySqlInsertStatement)stmt;
		String tableName = removeBackquote(insert.getTableName().getSimpleName()).toUpperCase();
		
		ctx.addTable(tableName);
		TableConfig tc = schema.getTables().get(tableName);
		if(tc == null) {
			String msg = "can't find table define in schema "
					+ tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		} else {
			String partitionColumn = tc.getPartitionColumn();
			
			if(partitionColumn != null) {//分片表
				//拆分表必须给出column list,否则无法寻找分片字段的值
				if(insert.getColumns() == null || insert.getColumns().size() == 0) {
					throw new SQLSyntaxErrorException("partition table, insert must provide ColumnList");
				}
				
				boolean isFound = false;
				if(insert.getValuesList().size() > 1 || insert.getQuery() != null) {
					//insert into .... select ....不能支持  
					//insert into table(id) values (),(),....不能支持
					String inf = "insert multi rows not supported! "; //TODO 此处可优化拆分到多个分片执行，从而支持一次插入多行
					LOGGER.warn(inf);
					throw new SQLNonTransientException(inf);
				}
				for(int i = 0; i < insert.getColumns().size(); i++) {
					if(partitionColumn.equalsIgnoreCase(removeBackquote(insert.getColumns().get(i).toString()))) {//找到分片字段
						isFound = true;
						String column = removeBackquote(insert.getColumns().get(i).toString());
						
						String value = removeBackquote(insert.getValues().getValues().get(i).toString());
						ctx.addShardingExpr(tableName, column, value);
						//只单分片键，找到了就返回
						break;
					}
				}
				if(!isFound) {//分片表的
					String msg = "bad insert sql (sharding column:"+ partitionColumn + " not provided," + stmt;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
			}
			
		}
		
	}
}
