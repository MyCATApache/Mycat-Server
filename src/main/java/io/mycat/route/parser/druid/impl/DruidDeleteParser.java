package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

import io.mycat.MycatServer;
import io.mycat.cache.CachePool;
import io.mycat.cache.DefaultLayedCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.util.StringUtil;

public class DruidDeleteParser extends DefaultDruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlDeleteStatement delete = (MySqlDeleteStatement)stmt;
		String tableName = StringUtil.removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
		ctx.addTable(tableName);

		//在解析SQL时清空该表的主键缓存
		TableConfig tableConfig = schema.getTables().get(tableName);
		if (tableConfig != null && !tableConfig.primaryKeyIsPartionKey()) {
			String cacheName = schema.getName() + "_" + tableName;
			cacheName = cacheName.toUpperCase();
			for (CachePool value : MycatServer.getInstance().getCacheService().getAllCachePools().values()) {
				value.clearCache(cacheName);
				value.getCacheStatic().reset();
			}
		}
	}
}

