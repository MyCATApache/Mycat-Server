package io.mycat.route;

import java.sql.SQLNonTransientException;
import java.util.Map;

import org.junit.Test;

import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.factory.RouteStrategyFactory;
import junit.framework.Assert;

public class DruidMysqlHavingTest
{
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

	public DruidMysqlHavingTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
		RouteStrategyFactory.init();
		routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
	}

	@Test
	public void testHaving() throws SQLNonTransientException {
		String sql = "select avg(offer_id) avgofferid, member_id from offer_detail group by member_id having avgofferid > 100";
		SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

		sql = "select avg(offer_id) avgofferid, member_id from offer_detail group by member_id having avg(offer_id) > 100";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

        sql = "select count(offer_id) countofferid, member_id from offer_detail group by member_id having countofferid > 100";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

        sql = "select count(offer_id) countofferid, member_id from offer_detail group by member_id having count(offer_id) > 100";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

	}
}
