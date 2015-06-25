package io.mycat.route;

import io.mycat.SystemConfig;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.mysql.MySQLFrontConnection;

import java.sql.SQLNonTransientException;

/**
 * 路由策略接口
 * @author wang.dw
 *
 */
public interface RouteStrategy {
	public RouteResultset route(SystemConfig sysConfig,
			SchemaConfig schema,int sqlType, String origSQL, String charset, MySQLFrontConnection sc, LayerCachePool cachePool)
			throws SQLNonTransientException;
}
