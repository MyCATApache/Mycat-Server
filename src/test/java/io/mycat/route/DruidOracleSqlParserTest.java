package io.mycat.route;

import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.config.loader.ConfigInitializer;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;

import java.sql.SQLNonTransientException;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;


public class DruidOracleSqlParserTest
{
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

	public DruidOracleSqlParserTest() {
		ConfigInitializer confInit = new ConfigInitializer(true);
		schemaMap = confInit.getSchemas();
	}

    @Test
    public void testInsertUpdate() throws Exception {
        SchemaConfig schema = schemaMap.get("oracledb");

   String     sql = "insert into offer1(group_id,offer_id,member_id)values(234,123,'abc')";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(-1, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());
        Assert.assertEquals(
                "insert into offer1(group_id,offer_id,member_id)values(234,123,'abc')",
                rrs.getNodes()[0].getStatement());

        sql = "update offer set name='x'";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(-1, rrs.getLimitSize());
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(-1, rrs.getNodes()[0].getLimitSize());

    }



    @Test
	public void testLimitToOraclePage() throws SQLNonTransientException {
		String sql = "select * from offer order by id desc limit 5,10";
		SchemaConfig schema = schemaMap.get("oracledb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());
        Assert.assertEquals("d_oracle2", rrs.getNodes()[1].getName());

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
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());
	}



    @Test
    public void testOraclePageSQL() throws SQLNonTransientException {
        String sql = "SELECT *\n" +
                "FROM (SELECT XX.*, ROWNUM AS RN \n" +
                " FROM (\n" +
                "SELECT *   FROM offer\n" +
                "                ) XX\n" +
                "        WHERE ROWNUM <= 15\n" +
                "        ) XXX\n" +
                "WHERE RN > 5 \n";
        SchemaConfig schema = schemaMap.get("oracledb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());
        Assert.assertEquals("d_oracle2", rrs.getNodes()[1].getName());

        sql = "SELECT *\n" +
                "FROM (SELECT XX.*, ROWNUM AS RN \n" +
                " FROM (\n" +
                "SELECT *   FROM offer1" +
                "                ) XX\n" +
                "        WHERE ROWNUM <= 15\n" +
                "        ) XXX\n" +
                "WHERE RN > 5 \n";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());

        sql="SELECT *\n" +
                "FROM (SELECT t.*, ROW_NUMBER() OVER (ORDER BY sid DESC) AS ROWNUM1\n" +
                "\tFROM offer t\n" +
                "\tWHERE sts <> 'N'\n" +
                "\t\t\n" +
                "\t) XX\n" +
                "WHERE ROWNUM1 > 5\n" +
                "\tAND ROWNUM1 <= 15\n";

        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());
        Assert.assertEquals("d_oracle2", rrs.getNodes()[1].getName());


        sql="SELECT *\n" +
                "FROM (SELECT t.*, ROW_NUMBER() OVER (ORDER BY sid DESC) AS ROWNUM1\n" +
                "\tFROM offer1  t\n" +
                "\tWHERE sts <> 'N'\n" +
                "\t\t\n" +
                "\t) XX\n" +
                "WHERE ROWNUM1 > 5\n" +
                "\tAND ROWNUM1 <= 15\n";

        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);

        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());


        sql="select sid from (select sid from offer ) where rownum<=10"  ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(sql,rrs.getNodes()[0].getStatement()) ;
        Assert.assertEquals("d_oracle1", rrs.getNodes()[0].getName());
        Assert.assertEquals("d_oracle2", rrs.getNodes()[1].getName());
    }
}
