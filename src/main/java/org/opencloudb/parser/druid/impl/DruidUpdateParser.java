package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.List;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

public class DruidUpdateParser extends DefaultDruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		if(ctx.getTables() != null && ctx.getTables().size() > 1 && !schema.isNoSharding()) {
			String msg = "multi table related update not supported,tables:" + ctx.getTables();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		MySqlUpdateStatement update = (MySqlUpdateStatement)stmt;
		String tableName = removeBackquote(update.getTableName().getSimpleName().toUpperCase());
		
		List<SQLUpdateSetItem> updateSetItem = update.getItems();
		TableConfig tc = schema.getTables().get(tableName);
		String partitionColumn = tc.getPartitionColumn();
		String joinKey = tc.getJoinKey();
		if(schema.isNoSharding()) {//整个schema都不分库或者该表不拆分
			RouterUtil.routeForTableMeta(rrs, schema, tableName, rrs.getStatement());
			rrs.setFinishedRoute(true);
			return;
		}
		
		if(tc.isGlobalTable() || (partitionColumn == null && joinKey == null)) {
			RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), rrs.getStatement());
			rrs.setFinishedRoute(true);
			return;
		}
		
		if(updateSetItem != null && updateSetItem.size() > 0) {
			boolean hasParent = (schema.getTables().get(tableName).getParentTC() != null);
			for(SQLUpdateSetItem item : updateSetItem) {
				String column = removeBackquote(item.getColumn().toString().toUpperCase());
				if(partitionColumn != null && partitionColumn.equals(column)) {
					String msg = "partion key can't be updated " + tableName + "->" + partitionColumn;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
				if(hasParent) {
					if(column.equals(joinKey)) {
						String msg = "parent relation column can't be updated " + tableName + "->" + joinKey;
						LOGGER.warn(msg);
						throw new SQLNonTransientException(msg);
					}
					rrs.setCacheAble(true);
				}
			}
		}
		
//		if(ctx.getTablesAndConditions().size() > 0) {
//			Map<String, Set<ColumnRoutePair>> map = ctx.getTablesAndConditions().get(tableName);
//			if(map != null) {
//				for(Map.Entry<String, Set<ColumnRoutePair>> entry : map.entrySet()) {
//					String column = entry.getKey();
//					Set<ColumnRoutePair> value = entry.getValue();
//					if(column.toUpperCase().equals(anObject))
//				}
//			}
//			
//		}
//		System.out.println();
		
		if(schema.getTables().get(tableName).isGlobalTable() && ctx.getTablesAndConditions().size() > 1) {
			throw new SQLNonTransientException("global table not supported multi table related update "+ tableName);
		}
	}
}
