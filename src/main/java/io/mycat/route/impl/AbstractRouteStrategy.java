package io.mycat.route.impl;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.LoadData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

public abstract class AbstractRouteStrategy implements RouteStrategy {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,int sqlType, String origSQL,
			String charset, MySQLFrontConnection sc, LayerCachePool cachePool) throws SQLNonTransientException {

		//process some before route logic
		if (beforeRouteProcess(schema, sqlType, origSQL, sc)) return null;

		// user handler
		String stmt = MycatServer.getInstance().getSqlInterceptor().interceptSQL(origSQL, sqlType);

		if (origSQL != stmt && LOGGER.isDebugEnabled()) {
			LOGGER.debug("sql intercepted to " + stmt + " from " + origSQL);
		}
		if (schema.isCheckSQLSchema()) {
			stmt = RouterUtil.removeSchema(stmt, schema.getName());
		}

		RouteResultset rrs = new RouteResultset(stmt, sqlType);

        if ( LOGGER.isDebugEnabled()&&origSQL.startsWith(LoadData.loadDataHint))
        {
          rrs.setCacheAble(false);//优化debug loaddata输出cache的日志会极大降低性能
        }

            //rrs携带ServerConnection的autocommit状态用于在sql解析的时候遇到select ... for update的时候动态设定RouteResultsetNode的canRunInReadDB属性
		if (sc != null ) {
			rrs.setAutocommit(sc.isAutocommit());
		}

		//ddl create deal
		if(ServerParse.DDL==sqlType){
			return RouterUtil.routeToDDLNode(rrs, sqlType, stmt,schema);
		}

		// check if there is sharding in schema
		if (schema.isNoSharding() && ServerParse.SHOW != sqlType) {
			rrs = RouterUtil.routeToSingleNode(rrs, schema.getDataNode(), stmt);
//			return RouterUtil.routeToSingleNode(rrs, schema.getDataNode(), stmt);
		} else {
			RouteResultset returnedSet=routeSystemInfo(schema, sqlType, stmt, rrs);
			if(returnedSet==null){
				rrs = routeNormalSqlWithAST(schema, stmt, rrs, charset, cachePool);
//				return routeNormalSqlWithAST(schema, stmt, rrs, charset, cachePool);
			}
		}

		return rrs;
	}

	private boolean beforeRouteProcess(SchemaConfig schema, int sqlType, String origSQL, MySQLFrontConnection sc) throws SQLNonTransientException {
		return RouterUtil.processWithMycatSeq(schema, sqlType, origSQL, sc) ||
                (sqlType == ServerParse.INSERT && RouterUtil.processERChildTable(schema, origSQL, sc)) ||
				(sqlType == ServerParse.INSERT && RouterUtil.processInsert(schema, sqlType, origSQL,sc));
	}

	/**
	 * 通过解析AST语法树类来寻找路由
	 * @param schema
	 * @param stmt
	 * @param rrs
	 * @param charset
	 * @param cachePool
	 * @return
	 * @throws SQLNonTransientException
	 */
	public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema,String stmt,RouteResultset rrs,String charset,LayerCachePool cachePool) throws SQLNonTransientException;

	/**
	 *
	 * @param schema
	 * @param sqlType
	 * @param stmt
	 * @param rrs
	 * @return
	 * @throws SQLSyntaxErrorException
	 */
	public abstract RouteResultset routeSystemInfo(SchemaConfig schema,int sqlType,String stmt,RouteResultset rrs) throws SQLSyntaxErrorException;

	/**
	 * show  之类的语句
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 * @throws SQLSyntaxErrorException
	 */
	public abstract RouteResultset analyseShowSQL(SchemaConfig schema,RouteResultset rrs, String stmt) throws SQLNonTransientException;

}
