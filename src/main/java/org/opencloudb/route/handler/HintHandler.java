package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;
import java.util.Map;

import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.server.ServerConnection;

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
