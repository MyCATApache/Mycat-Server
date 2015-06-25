package io.mycat.route.handler;

import io.mycat.SystemConfig;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.mysql.MySQLFrontConnection;
import io.mycat.route.RouteResultset;

import java.sql.SQLNonTransientException;

/**
 * 按照注释中包含指定类型的内容做路由解析
 * 
 */
public interface HintHandler {

	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset,
			MySQLFrontConnection sc, LayerCachePool cachePool,
			String hintSQLValue) throws SQLNonTransientException;
}
