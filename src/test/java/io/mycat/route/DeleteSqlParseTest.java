package io.mycat.route;

import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.parser.ServerParse;
import junit.framework.Assert;
import org.junit.Test;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * 测试删除
 * 
 * @author huangyiming
 *
 */
public class DeleteSqlParseTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

	public DeleteSqlParseTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
        RouteStrategyFactory.init();
        routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
	}

	@Test
	public void testDeleteToRoute() throws SQLNonTransientException {
		String sql = "delete t  from offer as t  ";
		SchemaConfig schema = schemaMap.get("config");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(128, rrs.getNodes().length);
	}

	@Test
	public void testInformationSchemaToRoute() throws SQLNonTransientException {
		String sql1 = "select SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from INFORMATION_SCHEMA.SCHEMATA";
		SchemaConfig schema = schemaMap.get("config");
		RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql1, null,
				null, cachePool);
		Assert.assertEquals(1, rrs.getNodes().length);

		String sql2 = "SELECT action_order, event_object_table, trigger_name, event_manipulation, event_object_table, definer, action_statement, action_timing\n" +
				"FROM information_schema.triggers\n" +
				"WHERE BINARY event_object_schema = 'config' AND BINARY event_object_table = 'offer'\n" +
				"ORDER BY event_object_table";
		rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql2, null,
				null, cachePool);
		Assert.assertEquals(1, rrs.getNodes().length);
	}


    
}
