package org.opencloudb.route;

import junit.framework.Assert;
import org.junit.Test;
import org.opencloudb.SimpleCachePool;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.route.util.RouterUtil;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * 修改内容
 * 
 * @author lxy
 *
 */
public class DruidOracleSqlParser
{
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

	public DruidOracleSqlParser() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
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
		
        sql="select * from offer1 order by id desc limit 5,10" ;
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
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
    }
}
