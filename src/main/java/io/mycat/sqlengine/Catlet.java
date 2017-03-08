package io.mycat.sqlengine;

import io.mycat.cache.LayerCachePool;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
/**
 * mycat catlet ,used to execute sql and return result to client,some like
 * database's procedure.
 * must implemented as a stateless class and can process many SQL concurrently 
 * 
 * @author wuzhih
 * 
 */
public interface Catlet {

	/*
	 * execute sql in EngineCtx and return result to client
	 */
	void processSQL(String sql, EngineCtx ctx);
	
	void route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset, MySQLFrontConnection sc,
			LayerCachePool cachePool) ;
	//void setRoute(RouteResultset rrs);
}
