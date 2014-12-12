package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;

import org.apache.log4j.Logger;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteStrategy;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.sqlengine.EngineCtx;
import org.opencloudb.sqlengine.demo.MyHellowJoin;

/**
 * 处理注释中类型为catlet 的情况,每个catlet为一个用户自定义Java代码类，用于进行复杂查询SQL（只能是查询SQL）的自定义执行过程，
 * 目前主要用于跨分片Join的人工智能编码
 */
public class HintCatletHandler implements HintHandler {

	private static final Logger LOGGER = Logger
			.getLogger(HintCatletHandler.class);

	private RouteStrategy routeStrategy;

	public HintCatletHandler() {
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
			LayerCachePool cachePool, String hintSQLValue)
			throws SQLNonTransientException {
		// sc.setEngineCtx ctx
		new MyHellowJoin().processSQL(realSQL, new EngineCtx(sc.getSession2()));
		return null;
		// schema = MycatServer.getInstance().getConfig().getSchemas()
		// .get(hintSQLValue);
		// if (schema != null) {
		// RouteResultset rrs = routeStrategy.route(sysConfig, schema,
		// sqlType, realSQL, charset, sc, cachePool);
		// return rrs;
		// } else {
		// String msg = "can't find schema:" + schema.getName();
		// LOGGER.warn(msg);
		// throw new SQLNonTransientException(msg);
		// }
	}
}
