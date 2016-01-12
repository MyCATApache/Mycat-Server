package io.mycat.route.handler;


import java.sql.SQLNonTransientException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;

import io.mycat.MycatServer;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.parser.ServerParse;

/**
 * 处理情况 sql hint: mycat:db_type=master/slave
 * 
 * @author digdeep@126.com
 */
// /*#mycat:db_type=master*/
// /*#mycat:db_type=slave*/
// 强制走 master 和 强制走 slave
public class HintMasterDBHandler implements HintHandler {
	
	private static final Logger LOGGER = Logger.getLogger(HintMasterDBHandler.class);

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
								int sqlType, String realSQL, String charset,
								MySQLFrontConnection sc, LayerCachePool cachePool,
								String hintSQLValue)throws SQLNonTransientException {
		
		RouteResultset rrs = RouteStrategyFactory.getRouteStrategy()
									.route(sysConfig, schema, sqlType, 
										realSQL, charset, sc, cachePool);
		
		Boolean isRouteToMaster = null;	// 默认不施加任何影响
		
		if(StringUtils.isNotBlank(hintSQLValue)){
			if(hintSQLValue.trim().equalsIgnoreCase("master"))
				isRouteToMaster = true;
			if(hintSQLValue.trim().equalsIgnoreCase("slave")){
				if(sqlType == ServerParse.DELETE || sqlType == ServerParse.INSERT
						||sqlType == ServerParse.REPLACE || sqlType == ServerParse.UPDATE
						|| sqlType == ServerParse.DDL){
					LOGGER.warn("should not use hint 'db_type' to route 'delete', 'insert', 'replace', 'update', 'ddl' to a slave db.");
					isRouteToMaster = null;	// 不施加任何影响好
				}else{
					isRouteToMaster = false;
				}
			}
		}
		
		if(isRouteToMaster == null){	// 默认不施加任何影响好
			LOGGER.warn(" sql hint 'db_type' error, ignore this hint.");
			return rrs;
		}
		
		if(isRouteToMaster)	// 强制走 master 
			rrs.setRunOnSlave(false);
		
		if(!isRouteToMaster)// 强制走slave
			rrs.setRunOnSlave(true);
		
		LOGGER.debug("isRouteToMaster:::::::::" + isRouteToMaster); // false
		return rrs;
	}

}