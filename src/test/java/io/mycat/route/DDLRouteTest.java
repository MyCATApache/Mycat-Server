package io.mycat.route;

import io.mycat.SimpleCachePool;
import io.mycat.cache.CacheService;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.config.loader.ConfigInitializer;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.config.node.TableConfig;
import io.mycat.server.parser.ServerParse;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DDLRouteTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
	protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

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
		CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table company(idd int)";
        sql = RouterUtil.getFixedSql(sql);
        String upsql = sql.toUpperCase();
        
        //TODO : modify by zhuam
        // 小写表名，需要额外转为 大写 做比较
        String tablename =  RouterUtil.getTableName(sql, RouterUtil.getCreateTablePos(upsql, 0));
        tablename = tablename.toUpperCase();
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
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table company
        sql = " drop table company";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getDropTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("company".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table COMPANY add COLUMN name int ;";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getAlterTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getTruncateTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);


    }

    @Test
    public void testDDLDefaultNode() throws Exception {
        SchemaConfig schema = schemaMap.get("solo1");
        CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table company(idd int)";
        sql = RouterUtil.getFixedSql(sql);
        String upsql = sql.toUpperCase();
        
        //TODO：modify by zhuam 小写表名，转为大写比较
        String tablename =  RouterUtil.getTableName(sql, RouterUtil.getCreateTablePos(upsql, 0));
        tablename = tablename.toUpperCase();     
        
        List<String> dataNodes = new ArrayList<>();
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        int nodeSize = dataNodes.size();
        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }

        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getDropTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table if exists COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getDropTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table COMPANY add COLUMN name int ;";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getAlterTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getTruncateTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }

        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);


    }
}
