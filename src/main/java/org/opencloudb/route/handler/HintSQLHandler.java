package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;

import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.route.RouteStrategy;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;

/**
 * 处理注释中 类型为sql的情况 （按照 注释中的sql做路由解析，而不是实际的sql）
 */
public class HintSQLHandler implements HintHandler {
	
	private RouteStrategy routeStrategy;
	
	public HintSQLHandler() {
		this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
	}

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool, String hintSQLValue)
			throws SQLNonTransientException {
		
		RouteResultset rrs = routeStrategy.route(sysConfig, schema, sqlType,
				hintSQLValue, charset, sc, cachePool);
		
		// 替换RRS中的SQL执行
		RouteResultsetNode[] oldRsNodes = rrs.getNodes();
		RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
		for (int i = 0; i < newRrsNodes.length; i++) {
			newRrsNodes[i] = new RouteResultsetNode(oldRsNodes[i].getName(),
					oldRsNodes[i].getSqlType(), realSQL);
		}
		rrs.setNodes(newRrsNodes);

		// 判断是否为调用存储过程的SQL语句，这里不能用SQL解析器来解析判断是否为CALL语句
		int rs = ServerParse.parse(realSQL);
		int realSQLType = rs & 0xff;
		if (ServerParse.CALL == realSQLType) {
			rrs.setCallStatement(true);
		}

		return rrs;
	}
}
