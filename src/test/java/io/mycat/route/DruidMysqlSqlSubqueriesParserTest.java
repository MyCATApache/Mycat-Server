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
		MycatServer.getInstance().getConfig().getSchemas().putAll(schemaMap);
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

		sql = "select * from subq1 where id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql = "select * from subq1 where id = 2";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        
        sql = "select * from subq1 where id = 3";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql = "select * from subq1 where name = 'name1'";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName()); 
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        // select list 子查询 
        sql = "select (select id from subq2) from subq1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName()); 
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        sql = "select (select id from subq3) from subq1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql = "select (select id from subq2 a where a.id = b.id) from subq1 a";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName()); 
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        
        
        
        
        
        
        sql = "select (select id from subq2 b where id = 1 and b.id = a.id ) from subq1 a";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql = "select (select b.id from subq2 b where b.id = a.id and b.id = 1 ) from subq1 a where a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        try{
	        sql = "select (select b.id from subq2 b where b.id = a.id and b.id = 1 ) from subq1 a where a.id = 2";
	        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
	                null, cachePool);
	        Assert.assertEquals(0, rrs.getNodes().length);
        } catch (Exception e) {
		}
        
        try{
	        sql = "select (select b.id from subq3 b where b.id = a.id and b.id = 1 ) from subq1 a where a.id = 1";
	        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
	                null, cachePool);
	        Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        sql = "select (select b.id from subq3 b where b.id = a.id and b.id = 2 ) from subq1 a where a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName()); 
        
        
        /*在 selectlist 中有多个子查询的情况.
        
        explain select (select id from subq2), (select id from subq3 where id = 1 ) from subq1;
        explain select (select id from subq2), (select id from subq3 where id = 2 ) from subq1;
        explain select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1;
        explain select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1 where id =1;
        explain select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1 where id =2;
        explain select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1 a where a.id =2;
        
        */

        
        try {
        	sql = "select (select id from subq2), (select id from subq3 where id = 1 ) from subq1";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        
        sql = "select (select id from subq2), (select id from subq3 where id = 2 ) from subq1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName()); 
        
        sql = "select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName()); 
        
        sql = "select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1 where id =1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName()); 

    	sql = "select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1 where id =1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName()); 
        
        sql = "select (select id from subq2 where id = 1), (select id from subq3 where id = 2) from subq1 a where a.id =1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());         
        
        /* 子查询包含多表的情况 
        explain select (select a.id from subq2 a,subq3 b where a.id = b.id) from subq1;
        explain select (select a.id from subq2 a,subq3 b where a.id = b.id and a.id = 1) from subq1;
        explain select (select a.id from subq2 a,subq3 b where a.id = b.id and a.id = 1) from subq1;
        */
        

        sql = "select (select a.id from subq2 a,subq3 b where a.id = b.id) from subq1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName()); 
        
        sql = "select (select a.id from subq2 a,subq3 b where a.id = b.id and a.id = 1) from subq1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName()); 
        
    	sql = "select (select a.id from subq2 a,subq3 b where a.id = b.id and a.id = 1) from subq1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        
       
        
        /* 子查询只包含一个的情况
        explain select * from (select * from subq1);
        explain select * from (select * from subq1 where id =1);
        explain select * from (select * from subq1) a where a.id = 1;  			这种查询会全扫描
        explain select * from (select * from subq1) a where a.name = 'abc';     这种查询会全扫描
        explain select * from (select * from subq1) a,subq2 b where a.id = b.id;
        explain select * from (select * from subq1) a,subq3 b where a.id = b.id;
        explain select * from (select * from subq1) a,subq2 b where a.id = b.id and a.id = 1;
        explain select * from (select * from subq1) a,subq3 b where a.id = b.id and b.id = 1;
        explain select * from (select * from subq1) a,subq2 b where a.id = b.id and a.id = 1 and b.id = 1;
        
         */
        /* 在 from 中有多个子查询的情况.

        explain select * from (select * from subq1)  a, (select * from subq2) b where a.id = b.id;

        explain select * from (select * from subq1)  a, (select * from subq2) b where a.id = 1 and b.id = 1 and a.id = b.id;

        explain select * from (select * from subq1)  a, (select * from subq2) b where a.id = 1 and b.id = 2 and a.id = b.id;

        explain select * from (select * from subq1 where id = 1 )  a, (select * from subq2) b where a.id = b.id;
         */

        /* 在子查询中包含多表的情况
        explain select * from (select a.id from subq2 a,subq3 b where a.id = b.id);
        explain select * from (select a.id from subq2 a,subq3 b where a.id = b.id  and  b.id = 1);
        explain select * from (select a.id from subq2 a,subq3 b where a.id = b.id  and  b.id = 2);
        explain select * from (select a.id from subq2 a,subq3 b where a.id = b.id) c,subq1 d;
        explain select * from (select a.id from subq2 a,subq3 b where a.id = b.id) c,subq1 d where c.id = d.id;
        explain select * from (select a.id from subq2 a,subq3 b where a.id = b.id) c,subq1 d where c.id = d.id and d.id = 1;
         */
        
        sql= "select * from (select * from subq1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
 
        sql= "select * from (select * from subq1 where id =1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from (select * from subq1) a where a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        sql= "select * from (select * from subq1) a where a.name = 'abc'";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        sql= "select * from (select * from subq1) a,subq2 b where a.id = b.id";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        sql= "select * from (select * from subq1) a,subq3 b where a.id = b.id";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select * from (select * from subq1) a,subq2 b where a.id = b.id and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        try {
        	sql= "select * from (select * from subq1) a,subq3 b where a.id = b.id and b.id = 1";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from (select * from subq1) a,subq2 b where a.id = b.id and a.id = 1 and b.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from (select * from subq1)  a, (select * from subq2) b where a.id = b.id";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql= "select * from (select * from subq1)  a, (select * from subq2) b where a.id = 1 and b.id = 1 and a.id = b.id";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql= "select * from (select * from subq1)  a, (select * from subq2) b where a.id = 1 and b.id = 2 and a.id = b.id";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql= "select * from (select * from subq1 where id = 1 )  a, (select * from subq2) b where a.id = b.id";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from (select a.id from subq2 a,subq3 b where a.id = b.id)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from (select a.id from subq2 a,subq3 b where a.id = b.id  and  b.id = 1)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}

        sql= "select * from (select a.id from subq2 a,subq3 b where a.id = b.id  and  b.id = 2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from (select a.id from subq2 a,subq3 b where a.id = b.id) c,subq1 d";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from (select a.id from subq2 a,subq3 b where a.id = b.id) c,subq1 d where c.id = d.id";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from (select a.id from subq2 a,subq3 b where a.id = b.id) c,subq1 d where c.id = d.id and d.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        ///////////////////////////////
        
        /* where  子句中包含单个子查询 exists形式 
        explain select * from subq1 where id =1;
        explain select * from subq1 a where (select 1 from subq2);
        explain select * from subq1 a where a.id in (select 1 from subq2 where id = 1);
        explain select * from subq1 a where (select 1 from subq3);
        explain select * from subq1 a where (select 1 from subq2 b) and a.id = 1;
        explain select * from subq1 a where (select 1 from subq2 b) and a.name = 'abc';
        explain select * from subq1 a where (select 1 from subq2 b where a.id = b.id and b.id = 1);
        explain select * from subq1 a where (select 1 from subq2 b where a.id = b.id and b.id = 1) 
        and a.id = 1;
        */
        /* where  子句中包含单个子查询 in 形式 
        explain select * from subq1 a where a.id in (select 1 from subq2);
        explain select * from subq1 a where a.id in (select 1 from subq2 where id = 1);
        explain select * from subq1 a where a.id in (select 1 from subq3);
        explain select * from subq1 a where a.id in (select 1 from subq2 b) and a.id = 1;
        explain select * from subq1 a where a.id in (select 1 from subq2 b) and a.name = 'abc';
        explain select * from subq1 a where a.id in (select 1 from subq2 b where a.id = b.id and b.id = 1);
        explain select * from subq1 a where a.id in (select 1 from subq2 b where a.id = b.id and b.id = 1) and a.id = 1;
        explain select * from subq1 a where a.id in (select 1 from subq2 b where a.id = b.id and b.id = 1) and a.id = 2;
         */
        /* 在where 中有多个子查询的情况. 
        explain select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 );
        explain select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 where id =1 );
        explain select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 ) and a.id = 1;
        explain select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 ) and a.id = 2;
         */
        /* 在where 子查询中，包含多表exists的情况. 
        explain select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id);
        explain select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 1);
        explain select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 2);
        explain select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 1;
        explain select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 2;
        explain select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 1;
        explain select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 2;
        */
        /* 在where 子查询中，包含多表 in的情况. 
        explain select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id);
        explain select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 1);
        explain select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 2);
        explain select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 1;
        explain select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 2;
        explain select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 1;
        explain select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 2;
         */
        
        sql= "select * from subq1 where id =1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where (select 1 from subq2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql= "select * from subq1 a where a.id in (select 1 from subq2 where id = 1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where (select 1 from subq3)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where (select 1 from subq2 b) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where (select 1 from subq2 b) and a.name = 'abc'";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql= "select * from subq1 a where (select 1 from subq2 b where a.id = b.id and b.id = 1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
	        sql= "select * from subq1 a where (select 1 from subq2 b where a.id = b.id and b.id = 1) and a.id =2";
	        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
	                null, cachePool);
	        Assert.assertEquals(1, rrs.getNodes().length);
	        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        } catch (Exception e) {
		}
        
    	sql= "select * from subq1 a where a.id in (select 1 from subq2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        

        sql= "select * from subq1 a where a.id in (select 1 from subq2 where id = 1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where a.id in (select 1 from subq3)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where a.id in (select 1 from subq2 b) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where a.id in (select 1 from subq2 b) and a.name = 'abc'";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql= "select * from subq1 a where a.id in (select 1 from subq2 b where a.id = b.id and b.id = 1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select * from subq1 a where a.id in (select 1 from subq2 b where a.id = b.id and b.id = 1) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where a.id in (select 1 from subq2 b where a.id = b.id and b.id = 1) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 )";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 where id =1 )";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 ) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where (select 1 from subq2 b where b.id = a.id) and a.id in(select id from subq3 ) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 1)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 1";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        try {
        	sql= "select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}

        sql= "select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where (select 1 from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 1)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 1";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        try {
        	sql= "select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 1) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        

        sql= "select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 1";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select * from subq1 a where a.id in (select b.id from subq2 b,subq3 c where b.id = c.id and c.id = 2) and a.id = 2";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        /* group by 中的单个子查询 
        explain select id from subq1 where 1 = 1 group by (select id from subq2);
        explain select id from subq1 where 1 = 1 group by (select id from subq2 where id = 2);
        explain select id from subq1 where 1 = 1 group by (select id from subq3 where id = 1);
        */

        /* group by 中的多表的查询
        explain select id from subq1 where 1 = 1 group by (select id from subq2 b,subq3 c where b.id = c.id);
        explain select id from subq1 where 1 = 1 group by (select id from subq2 b,subq3 c where b.id = c.id and b.id = 1);
        explain select id from subq1 where 1 = 1 group by (select id from subq2 b,subq3 c where b.id = c.id and b.id = 2);
         */
        
        sql= "select id from subq1 where 1 = 1 group by (select id from subq2) ";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        
        sql= "select id from subq1 where 1 = 1 group by (select id from subq2 where id = 2)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());

        try {
        	sql= "select id from subq1 where 1 = 1 group by (select id from subq3 where id = 1)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}

        sql= "select id from subq1 where 1 = 1 group by (select id from subq2 b,subq3 c where b.id = c.id)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        sql= "select id from subq1 where 1 = 1 group by (select id from subq2 b,subq3 c where b.id = c.id and b.id = 1)";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select id from subq1 where 1 = 1 group by (select id from subq2 b,subq3 c where b.id = c.id and b.id = 2)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        
        /* explain select id from subq1 where 1 =1 group by id having id in (select id from subq2); */
        try {
        	sql= "select id from subq1 where 1 =1 group by id having id in (select id from subq2)";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
		} catch (Exception e) {
		}
        
        /* order by 中的单个子查询 
        explain select id from subq1 order by (select id from subq2 where id = 1);
        explain select id from subq1 where id = 1 order by (select id from subq2 where id = 1);
        explain select id from subq1 where id = 1 order by (select id from subq3 where id = 1);
        */

        /* order by 中的多表查询 
        explain select id from subq1 where id = 1 order by (select id from subq2 b,subq3 c where b.id = c.id);
        explain select id from subq1 where id = 1 order by (select id from subq2 b,subq3 c where b.id = c.id and c.id = 1);
        explain select id from subq1 where id = 1 order by (select id from subq2 b,subq3 c where b.id = c.id and c.id = 2);
        */

        sql= "select id from subq1 order by (select id from subq2 where id = 1) ";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());
        
        sql= "select id from subq1 where id = 1 order by (select id from subq2 where id = 1) ";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select id from subq1 where id = 1 order by (select id from subq3 where id = 1) ";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        
        sql= "select id from subq1 where id = 1 order by (select id from subq2 b,subq3 c where b.id = c.id) ";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        try {
        	sql= "select id from subq1 where id = 1 order by (select id from subq2 b,subq3 c where b.id = c.id and c.id = 1) ";
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                    null, cachePool);
            Assert.assertEquals(0, rrs.getNodes().length);
		} catch (Exception e) {
		}
        
        
        sql= "select id from subq1 where id = 1 order by (select id from subq2 b,subq3 c where b.id = c.id and c.id = 2) ";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

	}
}