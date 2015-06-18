package io.mycat.route;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.server.ServerConnection;

import java.sql.SQLNonTransientException;

/**
 * 路由策略接口
 * @author wang.dw
 *
 */
public interface RouteStrategy {
	public RouteResultset route(SystemConfig sysConfig,
			SchemaConfig schema,int sqlType, String origSQL, String charset, ServerConnection sc, LayerCachePool cachePool)
			throws SQLNonTransientException;
}
