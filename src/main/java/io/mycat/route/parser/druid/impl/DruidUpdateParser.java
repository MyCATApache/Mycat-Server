package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import io.mycat.util.StringUtil;

import java.sql.SQLNonTransientException;
import java.util.List;

public class DruidUpdateParser extends DefaultDruidParser {
    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        //这里限制了update分片表的个数只能有一个
        if (ctx.getTables() != null && ctx.getTables().size() > 1 && !schema.isNoSharding()) {
            String msg = "multi table related update not supported,tables:" + ctx.getTables();
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        MySqlUpdateStatement update = (MySqlUpdateStatement) stmt;
        String tableName = StringUtil.removeBackquote(update.getTableName().getSimpleName().toUpperCase());

        List<SQLUpdateSetItem> updateSetItem = update.getItems();
        TableConfig tc = schema.getTables().get(tableName);

        if (RouterUtil.isNoSharding(schema, tableName)) {//整个schema都不分库或者该表不拆分
            RouterUtil.routeForTableMeta(rrs, schema, tableName, rrs.getStatement());
            rrs.setFinishedRoute(true);
            return;
        }

        String partitionColumn = tc.getPartitionColumn();
        String joinKey = tc.getJoinKey();
        if (tc.isGlobalTable() || (partitionColumn == null && joinKey == null)) {
            //修改全局表 update 受影响的行数
            RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), rrs.getStatement(), tc.isGlobalTable());
            rrs.setFinishedRoute(true);
            return;
        }


        confirmShardColumnNotUpdated(updateSetItem, schema, tableName, partitionColumn, joinKey, rrs);

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

        if (schema.getTables().get(tableName).isGlobalTable() && ctx.getRouteCalculateUnit().getTablesAndConditions().size() > 1) {
            throw new SQLNonTransientException("global table not supported multi table related update " + tableName);
        }
    }

    private void confirmShardColumnNotUpdated(List<SQLUpdateSetItem> updateSetItem,SchemaConfig schema,String tableName,String partitionColumn,String joinKey,RouteResultset rrs) throws SQLNonTransientException {
        if (updateSetItem != null && updateSetItem.size() > 0) {
            boolean hasParent = (schema.getTables().get(tableName).getParentTC() != null);
            for (SQLUpdateSetItem item : updateSetItem) {
                String column = StringUtil.removeBackquote(item.getColumn().toString().toUpperCase());
                //考虑别名，前面已经限制了update分片表的个数只能有一个，所以这里别名只能是分片表的
                if (column.contains(StringUtil.TABLE_COLUMN_SEPARATOR)) {
                    column = column.substring(column.indexOf(".") + 1).trim().toUpperCase();
                }
                if (partitionColumn != null && partitionColumn.equals(column)) {
                    String msg = "partion key can't be updated " + tableName + "->" + partitionColumn;
                    LOGGER.warn(msg);
                    throw new SQLNonTransientException(msg);
                }
                if (hasParent) {
                    if (column.equals(joinKey)) {
                        String msg = "parent relation column can't be updated " + tableName + "->" + joinKey;
                        LOGGER.warn(msg);
                        throw new SQLNonTransientException(msg);
                    }
                    rrs.setCacheAble(true);
                }
            }
        }
    }
}
