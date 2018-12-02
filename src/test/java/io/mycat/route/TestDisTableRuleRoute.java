package io.mycat.route;

import io.mycat.MycatServer;
import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import java.sql.SQLNonTransientException;
import java.util.Map;
import org.junit.Test;

/**
 * @author liunan  by 2018/12/2
 */
public class TestDisTableRuleRoute {



    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();

    public TestDisTableRuleRoute() {
        String schemaFile = "/route/disRoute/schema.xml";
        String ruleFile = "/route/disRoute/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
        MycatServer.getInstance().getConfig().getSchemas().putAll(schemaMap);
        RouteStrategyFactory.init();
    }

    @Test
    public void testDisTableInsert() throws SQLNonTransientException {
        String sql = "insert into sqtestmonth (id,name,create_time) values(1,'sq1', '2017-5-12')";
//        String sql = "insert into sqtestmonth (name,create_time) Select name , create_time from sqtestmonth";
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = RouteStrategyFactory
                .getRouteStrategy()
                .route(new SystemConfig(),schema, -1, sql, null,
                        null, cachePool);
        System.out.println(rrs);
    }

}
