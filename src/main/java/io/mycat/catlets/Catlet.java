package io.mycat.catlets;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.server.ServerConnection;
import io.mycat.sqlengine.EngineCtx;
/**
 * mycat catlet，用于执行sql并将结果返回给客户端，有些类似于数据库的过程。
 * 必须实现为无状态类，并且可以并发地处理多个SQL
 *
 * mycat catlet ,used to execute sql and return result to client,some like
 * database's procedure.
 * must implemented as a stateless class and can process many SQL concurrently 
 * 
 * @author wuzhih
 * 
 */
public interface Catlet {

	/**
	 * 在EngineCtx中执行sql并将结果返回给客户端
	 */
	void processSQL(String sql, EngineCtx ctx);
	
	void route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool) ;
	//void setRoute(RouteResultset rrs);
}
