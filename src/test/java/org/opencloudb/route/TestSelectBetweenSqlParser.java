package org.opencloudb.route;

import java.sql.SQLNonTransientException;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.opencloudb.SimpleCachePool;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.factory.RouteStrategyFactory;

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
	}

	@Test
	public void testBetweenSqlRoute() throws SQLNonTransientException {
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
		rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
				null, cachePool);
		Assert.assertEquals(5, rrs.getNodes().length);
		
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
	}
}
