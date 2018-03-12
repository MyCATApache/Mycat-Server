package io.mycat.route.handler;


import java.sql.SQLNonTransientException;
import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;

/**
 * 处理情况 sql hint: mycat:db_type=master/slave<br/>
 * 后期可能会考虑增加 mycat:db_type=slave_newest，实现走延迟最小的slave
 * @author digdeep@126.com
 */
// /*#mycat:db_type=master*/
// /*#mycat:db_type=slave*/
// /*#mycat:db_type=slave_newest*/
// 强制走 master 和 强制走 slave
public class HintMasterDBHandler implements HintHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HintMasterDBHandler.class);

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType, 
			String realSQL, String charset,
			ServerConnection sc, LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
			throws SQLNonTransientException {
		
//		LOGGER.debug("realSQL: " + realSQL); // select * from travelrecord limit 1
//		LOGGER.debug("sqlType: " + sqlType); // 7
//		LOGGER.debug("schema.getName(): " + schema.getName()); // TESTDB
//		LOGGER.debug("schema.getName(): " + schema.getDataNode()); // null
//		LOGGER.debug("hintSQLValue: " + hintSQLValue); // master/slave
		
		RouteResultset rrs = RouteStrategyFactory.getRouteStrategy()
									.route(sysConfig, schema, sqlType, 
										realSQL, charset, sc, cachePool);
		
		LOGGER.debug("schema.rrs(): " + rrs); // master
		Boolean isRouteToMaster = null;	// 默认不施加任何影响
		
		LOGGER.debug("hintSQLValue:::::::::" + hintSQLValue); // slave
		
		if(hintSQLValue != null && !hintSQLValue.trim().equals("")){
			if(hintSQLValue.trim().equalsIgnoreCase("master")) {
				isRouteToMaster = true;
			}
			if(hintSQLValue.trim().equalsIgnoreCase("slave")){
//				if(rrs.getCanRunInReadDB() != null && !rrs.getCanRunInReadDB()){
//					isRouteToMaster = null;
//					LOGGER.warn(realSQL + " can not run in slave.");
//				}else{
//					isRouteToMaster = false;
//				}
				if(sqlType == ServerParse.DELETE || sqlType == ServerParse.INSERT
						||sqlType == ServerParse.REPLACE || sqlType == ServerParse.UPDATE
						|| sqlType == ServerParse.DDL){
					LOGGER.error("should not use hint 'db_type' to route 'delete', 'insert', 'replace', 'update', 'ddl' to a slave db.");
					isRouteToMaster = null;	// 不施加任何影响
				}else{
					isRouteToMaster = false;
				}
			}
		}
		
		if(isRouteToMaster == null){	// 默认不施加任何影响
			LOGGER.warn(" sql hint 'db_type' error, ignore this hint.");
			return rrs;
		}
		
		if(isRouteToMaster)	 {// 强制走 master
			rrs.setRunOnSlave(false);
		}
		
		if(!isRouteToMaster) {// 强制走slave
			rrs.setRunOnSlave(true);
		}
		
		LOGGER.debug("rrs.getRunOnSlave():" + rrs.getRunOnSlave());
		return rrs;
	}

}