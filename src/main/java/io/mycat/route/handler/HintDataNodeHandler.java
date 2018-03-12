package io.mycat.route.handler;

import java.sql.SQLNonTransientException;
import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;

/**
 * 处理注释中类型为datanode 的情况
 * 
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HintSchemaHandler.class);

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
