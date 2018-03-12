package io.mycat.route;

import java.io.IOException;
import java.sql.SQLNonTransientException;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import io.mycat.MycatServer;
import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.ServerConnection;

/**
 * 修改内容
 * 
 * @author lxy
 *
 */
public class TestSelectBetweenSqlParser {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();

	public TestSelectBetweenSqlParser() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
		MycatServer.getInstance().getConfig().getSchemas().putAll(schemaMap);
		RouteStrategyFactory.init();
	}

	@Test
	public void testBetweenSqlRoute() throws SQLNonTransientException, IOException {
		String sql = "select * from offer_detail where offer_id between 1 and 33";
		SchemaConfig schema = schemaMap.get("cndb");
		RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
				null, cachePool);
		Assert.assertEquals(5, rrs.getNodes().length);
		
		sql = "select * from offer_detail where col_1 = 33 and offer_id between 1 and 33 and col_2 = 18";
		schema = schemaMap.get("cndb");
		rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
				null, cachePool);
		Assert.assertEquals(5, rrs.getNodes().length);
		
//		sql = "select b.* from offer_date b join  offer_detail a on a.id=b.id " +
//				"where b.col_date between '2014-02-02' and '2014-04-12' and col_1 = 3 and offer_id between 1 and 33";
		
		
		sql = "select b.* from offer_detail a  join  offer_date b on a.id=b.id " +
				"where b.col_date between '2014-02-02' and '2014-04-12' and col_1 = 3 and offer_id between 1 and 33";
//		sql = "select a.* from offer_detail a join offer_date b on a.id=b.id " +
//				"where b.col_date = '2014-04-02' and col_1 = 33 and offer_id =1";
		schema = schemaMap.get("cndb");
		// 两个路由规则不一样的表现在 走catlet. 不再取交集, catlet 测试时需要前端连接.这里注释掉.
//		rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
//				null, cachePool);
//		Assert.assertEquals(2, rrs.getNodes().length);    //这里2个表都有条件路由，取的是交集, 
		
		//确认大于小于操作符
		sql = "select b.* from  offer_date b " +
				"where b.col_date > '2014-02-02'";
//		sql = "select a.* from offer_detail a join offer_date b on a.id=b.id " +
//				"where b.col_date = '2014-04-02' and col_1 = 33 and offer_id =1";
		schema = schemaMap.get("cndb");
		rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
				null, cachePool);
		Assert.assertEquals(128, rrs.getNodes().length);
		
		sql = "select * from offer_date where col_1 = 33 and col_date between '2014-01-02' and '2014-01-12'";
		schema = schemaMap.get("cndb");
		rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
				null, cachePool);
		Assert.assertEquals(2, rrs.getNodes().length);

		sql = "select * from offer_date a where col_1 = 33 and a.col_date between '2014-01-02' and '2014-01-12'";
		schema = schemaMap.get("cndb");
		rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
				null, cachePool);
		Assert.assertEquals(2, rrs.getNodes().length);
	}
}
