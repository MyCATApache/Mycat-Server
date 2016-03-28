package io.mycat.route.handler;

import java.sql.SQLNonTransientException;
import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.ServerConnection;

/**
 * 处理注释中类型为schema 的情况（按照指定schema做路由解析）
 */
public class HintSchemaHandler implements HintHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(HintSchemaHandler.class);

	private RouteStrategy routeStrategy;
    
    public HintSchemaHandler() {
		this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
	}
	/**
	 * 从全局的schema列表中查询指定的schema是否存在， 如果存在则替换connection属性中原有的schema，
	 * 如果不存在，则throws SQLNonTransientException，表示指定的schema 不存在
	 * 
	 * @param sysConfig
	 * @param schema
	 * @param sqlType
	 * @param realSQL
	 * @param charset
	 * @param info
	 * @param cachePool
	 * @param hintSQLValue
	 * @return
	 * @throws SQLNonTransientException
	 */
	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool, String hintSQLValue,int hintSqlType, Map hintMap)
			throws SQLNonTransientException {
	    SchemaConfig tempSchema = MycatServer.getInstance().getConfig().getSchemas().get(hintSQLValue);
		if (tempSchema != null) {
			return routeStrategy.route(sysConfig, tempSchema, sqlType, realSQL, charset, sc, cachePool);
		} else {
			String msg = "can't find hint schema:" + hintSQLValue;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
	}
}
