package org.opencloudb.route;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.opencloudb.SimpleCachePool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.server.parser.ServerParse;

public class HintTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
	protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("fdbparser");

	public HintTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
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
