package io.mycat.route;

import io.mycat.MycatServer;
import io.mycat.SimpleCachePool;
import io.mycat.backend.PhysicalDBNode;
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

public class DDLRouteTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
	protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("fdbparser");

	public DDLRouteTest() {
		ConfigInitializer confInit = new ConfigInitializer(true);
		schemaMap = confInit.getSchemas();
	}

	/**
     * ddl deal test
     *
     * @throws Exception
     */
    @Test
    public void testDDL() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        Map<String,PhysicalDBNode> dataNodes = MycatServer.getInstance().getConfig().getDataNodes();
		int nodeSize = dataNodes.size();
		CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table test(idd int)";
        int rs = ServerParse.parse(sql);
		int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table test";
        rs = ServerParse.parse(sql);
		sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table add COLUMN name int ;";
        rs = ServerParse.parse(sql);
		sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table test";
        rs = ServerParse.parse(sql);
		sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue(rrs.getNodes().length == nodeSize);


    }
}
