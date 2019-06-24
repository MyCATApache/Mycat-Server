package io.mycat.route;

import java.sql.SQLNonTransientException;
import java.util.Map;

import org.junit.Test;

import io.mycat.MycatServer;
import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import junit.framework.Assert;

@SuppressWarnings("deprecation")
public class DruidMysqlSqlSubqueriesParserTest
{
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

	public DruidMysqlSqlSubqueriesParserTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
		MycatServer.getInstance().getConfig().getSchemas().putAll(schemaMap);
        RouteStrategyFactory.init();
        routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
	}

	
	@Test
	public void testSubQueries() throws SQLNonTransientException {
		//子查询测试需要构建ServerConnection. 暂时不在单元测试中体现.以测试报告的形式体现
	}
}