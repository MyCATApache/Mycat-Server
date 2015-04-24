package org.opencloudb.sqlengine;

import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.server.ServerConnection;
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
			int sqlType, String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool) ;
	//void setRoute(RouteResultset rrs);
}
