package io.mycat.route;

import io.mycat.SimpleCachePool;
import io.mycat.cache.CacheService;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.parser.ServerParse;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class HintDBTypeTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
	protected RouteStrategy routeStrategy;

	public HintDBTypeTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
        RouteStrategyFactory.init();
        routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
	}
	/**
     * 测试注解
     *
     * @throws Exception
     */
    @Test
    public void testHint() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
       //使用注解（新注解，/*!mycat*/），runOnSlave=false 强制走主节点
        String sql = "/*!mycat:db_type=master*/select * from employee where sharding_id=1";
        CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, ServerParse.SELECT, sql, "UTF-8", null);
        Assert.assertTrue(!rrs.getRunOnSlave());

        //使用注解（新注解，/*#mycat*/），runOnSlave=false 强制走主节点
        sql = "/*#mycat:db_type=master*/select * from employee where sharding_id=1";
        rrs = routerService.route(new SystemConfig(), schema, ServerParse.SELECT, sql, "UTF-8", null);
        Assert.assertTrue(!rrs.getRunOnSlave());
        
        //使用注解（新注解，/*mycat*/），runOnSlave=false 强制走主节点
        sql = "/*mycat:db_type=master*/select * from employee where sharding_id=1";
        rrs = routerService.route(new SystemConfig(), schema, ServerParse.SELECT, sql, "UTF-8", null);
        Assert.assertTrue(!rrs.getRunOnSlave());
        
        //不使用注解，runOnSlave=null, 根据读写分离策略走主从库
        sql = "select * from employee where sharding_id=1";
        rrs = routerService.route(new SystemConfig(), schema, ServerParse.SELECT, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getRunOnSlave()==null);
    }
}
