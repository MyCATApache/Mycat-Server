package io.mycat.route.handler;


import java.sql.SQLNonTransientException;

import org.apache.log4j.Logger;

import io.mycat.MycatServer;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;

/**
 * 处理注释中类型为datanode 的情况
 * 
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {
	
	private static final Logger LOGGER = Logger.getLogger(HintSchemaHandler.class);

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset,
			MySQLFrontConnection sc, LayerCachePool cachePool,
			String hintSQLValue)
					throws SQLNonTransientException {
		
		String stmt = realSQL;
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("route datanode sql hint from " + stmt);
		}
		
		RouteResultset rrs = new RouteResultset(stmt, sqlType);		
		PhysicalDBNode dataNode = MycatServer.getInstance().getConfig().getDataNodes().get(hintSQLValue);
		if (dataNode != null) {			
			rrs = RouterUtil.routeToSingleNode(rrs, dataNode.getName(), stmt);
		} else {
			String msg = "can't find hint datanode:" + hintSQLValue;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		
		return rrs;
	}

}