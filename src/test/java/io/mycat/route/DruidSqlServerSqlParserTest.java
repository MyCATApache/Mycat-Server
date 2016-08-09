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
import junit.framework.Assert;

public class DruidSqlServerSqlParserTest
{
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

	public DruidSqlServerSqlParserTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
        RouteStrategyFactory.init();
        routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
	}

	@Test
	public void testLimitToSqlServerPage() throws SQLNonTransientException {
		String sql = "select * from offer order by id desc limit 5,10";
		SchemaConfig schema = schemaMap.get("sqlserverdb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());
        Assert.assertEquals("sqlserver_2", rrs.getNodes()[1].getName());


        sql= rrs.getNodes()[0].getStatement() ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(15, rrs.getLimitSize());
		
        sql="select * from offer1 order by id desc limit 5,10" ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());

	}



    @Test
    public void testSqlServerPageSQL() throws SQLNonTransientException {
        String sql = "SELECT *\n" +
                "FROM (SELECT sid, ROW_NUMBER() OVER (ORDER BY sid DESC) AS ROWNUM\n" +
                "\tFROM offer \n" +
                "\tWHERE sts <> 'N'\n" +
                "\t\t\t) XX\n" +
                "WHERE ROWNUM > 5\n" +
                "\tAND ROWNUM <= 15\n";
        SchemaConfig schema = schemaMap.get("sqlserverdb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());
        Assert.assertEquals("sqlserver_2", rrs.getNodes()[1].getName());

        sql = "SELECT *\n" +
                "FROM (SELECT sid, ROW_NUMBER() OVER (ORDER BY sid DESC) AS ROWNUM\n" +
                "\tFROM offer1 \n" +
                "\tWHERE sts <> 'N'\n" +
                "\t\t\t) XX\n" +
                "WHERE ROWNUM > 5\n" +
                "\tAND ROWNUM <= 15\n";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());


         sql="select * from ( select row_number()over(order by tempColumn)tempRowNumber,* from ( select top \n" +
                 "15 tempColumn=0, sid \n" +
                 "from offer  where sts<>'N' and asf like '%'+'akka'+'%' order by sid  )t )tt where tempRowNumber>5";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());
        Assert.assertEquals("sqlserver_2", rrs.getNodes()[1].getName());



        sql="select * from ( select row_number()over(order by tempColumn)tempRowNumber,* from ( select top \n" +
                "15 tempColumn=0, sid \n" +
                "from offer1  where sts<>'N' and asf like '%'+'akka'+'%' order by sid  )t )tt where tempRowNumber>5";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());


        sql="SELECT TOP 10 sid  \n" +
                " FROM offer  where sts<>'N' and asf like '%'+'akka'+'%' \n" +
                " ORDER BY sid desc"  ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());
        Assert.assertEquals("sqlserver_2", rrs.getNodes()[1].getName());




    }



    @Test
    public void testTopPageSQL() throws SQLNonTransientException {
        SchemaConfig schema = schemaMap.get("sqlserverdb");
        RouteResultset rrs = null;

    String    sql="SELECT TOP 10  *  \n" +
                " FROM offer1  where sts<>'N' and asf like '%'+'akka'+'%' \n" +
                " ORDER BY sid desc"  ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);

        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());


        sql="SELECT TOP 10  offer1.name,offer1.id  \n" +
                " FROM offer1  where sts<>'N' and asf like '%'+'akka'+'%' \n" +
                " ORDER BY sid desc"  ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);

        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("sqlserver_1", rrs.getNodes()[0].getName());

    }
}
