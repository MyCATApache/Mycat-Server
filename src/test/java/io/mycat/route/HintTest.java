package io.mycat.route;

import io.mycat.SimpleCachePool;
import io.mycat.cache.CacheService;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.config.loader.ConfigInitializer;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.parser.ServerParse;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class HintTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
	protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("fdbparser");

	public HintTest() {
		ConfigInitializer confInit = new ConfigInitializer(true);
		schemaMap = confInit.getSchemas();
	}
	/**
     * 测试注解
     *
     * @throws Exception
     */
    @Test
    public void testHint() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        //使用注解（新注解，/*后面没有空格），路由到1个节点
        String sql = "/*!mycat: sql = select * from employee where sharding_id = 10010 */select * from employee";
        CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, ServerParse.SELECT, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getNodes().length == 1);

        //使用注解（新注解，/*后面有空格），路由到1个节点
        sql = "/*#mycat: sql = select * from employee where sharding_id = 10000 */select * from employee";
        rrs = routerService.route(new SystemConfig(), schema, ServerParse.SELECT, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getNodes().length == 1);

        //不用注解，路由到2个节点
        sql = "select * from employee";
        rrs = routerService.route(new SystemConfig(), schema, ServerParse.SELECT, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getNodes().length == 2);
    }
}
