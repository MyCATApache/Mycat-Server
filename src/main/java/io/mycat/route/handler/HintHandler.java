package io.mycat.route.handler;

import java.sql.SQLNonTransientException;
import java.util.Map;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteResultset;
import io.mycat.server.ServerConnection;

/**
 * 按照注释中包含指定类型的内容做路由解析
 * 
 */
public interface HintHandler {

	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
                                int sqlType, String realSQL, String charset, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
			throws SQLNonTransientException;
}
