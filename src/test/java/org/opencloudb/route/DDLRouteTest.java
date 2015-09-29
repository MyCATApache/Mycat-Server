package org.opencloudb.route;

import junit.framework.Assert;
import org.junit.Test;
import org.opencloudb.SimpleCachePool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.parser.ServerParse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DDLRouteTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
	//protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

	public DDLRouteTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
	}

	/**
     * ddl deal test
     *
     * @throws Exception
     */
    @Test
    public void testDDL() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
		CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table t5(idd int)";
        sql = RouterUtil.getFixedSql(sql);
        String tablename =  RouterUtil.getTableName(sql, RouterUtil.getCreateTablePos(sql, 0));
        List<String> dataNodes = new ArrayList<>();
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        int nodeSize = dataNodes.size();

        int rs = ServerParse.parse(sql);
		int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("T5".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table t5";
        sql = RouterUtil.getFixedSql(sql);
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getDropTablePos(sql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        rs = ServerParse.parse(sql);
		sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("T5".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table t5 add COLUMN name int ;";
        sql = RouterUtil.getFixedSql(sql);
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getAlterTablePos(sql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
		sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("T5".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table t5";
        sql = RouterUtil.getFixedSql(sql);
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getTruncateTablePos(sql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
		sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("T5".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);


    }
}
