package io.mycat.route;

import io.mycat.MycatServer;
import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLRuleLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import java.sql.SQLNonTransientException;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

/**
 * @author liunan  by 2018/8/29
 */
public class TestSubTableRuleRoute {

    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();

    public TestSubTableRuleRoute() {
        String schemaFile = "/route/sub_tables/schema.xml";
        String ruleFile = "/route/sub_tables/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
        MycatServer.getInstance().getConfig().getSchemas().putAll(schemaMap);
        RouteStrategyFactory.init();
    }


    @Test
    public void testSelect() throws SQLNonTransientException {
        String sql = "select * from offer_detail where offer_id between 1 and 33";
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(5, rrs.getNodes().length);
    }

    @Test
    public void testInsert() throws SQLNonTransientException {
        String sql = "insert into sqtestmonth (id,name,create_time) values(1,'sq1', '2017-5-12')";
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = RouteStrategyFactory
                .getRouteStrategy()
                .route(new SystemConfig(),schema, -1, sql, null,
                null, cachePool);
        System.out.println(rrs.getNodes()[0]);
        Assert.assertTrue(rrs.getNodes()[0].getStatement().contains("sqtestmonth20175"));
        Assert.assertEquals(2, rrs.getNodes().length);
    }

    @Test
    public void testSelect2() throws SQLNonTransientException {
        String sql = "select * from sqtestmonth where id = 1 and create_time = '2017-5-12'";
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = RouteStrategyFactory
                .getRouteStrategy()
                .route(new SystemConfig(),schema, -1, sql, null,
                        null, cachePool);
        System.out.println(rrs.getNodes()[0]);
        Assert.assertTrue(rrs.getNodes()[0].getStatement().contains("sqtestmonth20175"));
        Assert.assertEquals(2, rrs.getNodes().length);
    }
}
