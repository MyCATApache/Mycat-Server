package io.mycat.route;

import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.config.loader.ConfigInitializer;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;

import java.sql.SQLNonTransientException;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class DruidMysqlSqlParserTest
{
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

	public DruidMysqlSqlParserTest() {
		ConfigInitializer confInit = new ConfigInitializer(true);
		schemaMap = confInit.getSchemas();
	}

	@Test
	public void testLimitPage() throws SQLNonTransientException {
		String sql = "select * from offer order by id desc limit 5,10";
		SchemaConfig schema = schemaMap.get("mysqldb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql= rrs.getNodes()[0].getStatement() ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(15, rrs.getLimitSize());

        sql="select * from offer1 order by id desc limit 5,10" ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());


        sql="select * from offer1 order by id desc limit 10" ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());

	}




}
