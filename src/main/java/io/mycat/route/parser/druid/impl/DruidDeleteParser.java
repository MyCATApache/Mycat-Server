package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

import io.mycat.MycatServer;
import io.mycat.cache.DefaultLayedCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.util.StringUtil;

import java.sql.SQLNonTransientException;

/**
 * Druid Delete 解析器
 */
public class DruidDeleteParser extends DefaultDruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlDeleteStatement delete = (MySqlDeleteStatement)stmt;
		String tableName = StringUtil.removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
		ctx.addTable(tableName);

		//在解析SQL时清空该表的主键缓存
		DefaultLayedCachePool tableID2DataNodeCache=(DefaultLayedCachePool) MycatServer.getInstance().getCacheService()
				.getCachePool("TableID2DataNodeCache");
		tableID2DataNodeCache.clearCache(schema.getName().toLowerCase()+"_"+tableName.toUpperCase());
		tableID2DataNodeCache.getCacheStatic().reset();

	}
}

