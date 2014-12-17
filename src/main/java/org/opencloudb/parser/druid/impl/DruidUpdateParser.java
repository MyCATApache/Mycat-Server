package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.List;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.route.RouteResultset;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

public class DruidUpdateParser extends DefaultDruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlUpdateStatement update = (MySqlUpdateStatement)stmt;
		String tableName = removeBackquote(update.getTableName().getSimpleName().toUpperCase());
		
		List<SQLUpdateSetItem> updateSetItem = update.getItems();
		if(updateSetItem != null && updateSetItem.size() > 0) {
			String partitionColumn = schema.getTables().get(tableName).getPartitionColumn();
			String joinKey = schema.getTables().get(tableName).getJoinKey();
			boolean hasParent = (schema.getTables().get(tableName).getParentTC() != null);
			for(SQLUpdateSetItem item : updateSetItem) {
				String column = removeBackquote(item.getColumn().toString().toUpperCase());
				if(partitionColumn.equals(column)) {
					String msg = "partion key can't be updated: " + tableName + " -> " + partitionColumn;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
				if(hasParent) {
					if(column.equals(joinKey)) {
						String msg = "parent relation column can't be updated " + tableName + " -> " + joinKey;
						LOGGER.warn(msg);
						throw new SQLNonTransientException(msg);
					}
				}
			}
		}
		
		if(schema.getTables().get(tableName).isGlobalTable() && ctx.getTablesAndConditions().size() > 1) {
			throw new SQLNonTransientException("global table not supported multi table related update "+ tableName);
		}
	}
}
