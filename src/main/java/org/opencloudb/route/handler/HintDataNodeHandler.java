package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.ServerConnection;

/**
 * 处理注释中类型为datanode 的情况
 * 
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {
	
	private static final Logger LOGGER = Logger.getLogger(HintSchemaHandler.class);

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType, String realSQL,
			String charset, ServerConnection sc, LayerCachePool cachePool, String hintSQLValue,int hintSqlType, Map hintMap)
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
