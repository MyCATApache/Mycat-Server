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
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.parser.ServerParse;
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
        RouteStrategyFactory.init();
        routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
	}

	
	@Test
	public void testSubQueries() throws SQLNonTransientException {

		String sql = "select * from subq1";
		SchemaConfig schema = schemaMap.get("subQueries");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

		
        sql= "select * from subq1 where (select * from subq2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
 
		sql = "select * from subq1 where id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from subq1 where (select * from subq2 where id = 1) ";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from subq1 a where  a.id in (select * from subq2 where id = 1) ";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from subq1 where (select * from subq2) and id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from subq1 a where a.id in (select * from subq2) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from subq1 where (select * from subq2 where id = 1) and id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where a.id in (select * from subq2 b where b.id = 1) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
       try {
    	   sql= "select * from subq1 a where (select * from subq2 b where b.id = 2) and a.id = 1";
           rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                   null, cachePool);
           Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}

       	try {
       		sql= "select * from subq1 a where a.id in (select * from subq2 b where b.id = 2) and a.id = 1";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        // 出现多个子查询语句 的支持情况

        sql= "select * from subq1 where (select * from subq2) and (select * from subq3)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from subq1 a where (select * from subq2 b) and (select * from subq3 c) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

       	try {
       		sql= "select * from subq1 a where (select * from subq2 b) and (select * from subq3 c) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
       	try {
       		sql= "select * from subq1 where (select * from subq2 where id = 1) and (select * from subq3 where id = 1)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
       	
       	sql= "select * from subq1 where (select * from subq2 where id = 1) and (select * from subq3 where id = 2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from subq1 a where a.id in (select * from subq2 b) and (select * from subq3 c) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

       	try {
       		sql= "select * from subq1 a where (select * from subq2 b) and a.id in (select * from subq3 c) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
       	
       	sql= "select * from subq1 a where a.id in (select * from subq2 b where b.id = 1) and (select * from subq3 c where c.id = 2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        
        //测试子查询包含多表的情况
       	try {
       		sql= "select * from subq1 a where (select * from subq2 b,subq3 c where b.id = c.id and c.id = 1)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
       	sql= "select * from subq3 a where (select * from subq2 b,subq1 c where b.id = c.id and c.id = 1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

	}
}
