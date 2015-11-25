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
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.parser.ServerParse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DDLRouteTest {
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
	protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

	public DDLRouteTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
	}
	
	 @Test
	 public void testSpecialCharDDL() throws Exception {
		 SchemaConfig schema = schemaMap.get("TESTDB");
			CacheService cacheService = new CacheService();
	        RouteService routerService = new RouteService(cacheService);
	        
	        // drop table test
	        String  sql = " ALTER TABLE COMPANY\r\nADD COLUMN TEST  VARCHAR(255) NULL AFTER CREATE_DATE,\r\nDEFAULT CHARACTER SET DEFAULT";
	        sql = RouterUtil.getFixedSql(sql);
	        List<String> dataNodes = new ArrayList<>();
	        String  tablename =  RouterUtil.getTableName(sql, RouterUtil.getAlterTablePos(sql, 0));
	        Map<String, TableConfig>  tables = schema.getTables();
	        TableConfig tc;
	        if (tables != null && (tc = tables.get(tablename)) != null) {
	            dataNodes = tc.getDataNodes();
	        }
	        int nodeSize  = dataNodes.size();

	        int rs = ServerParse.parse(sql);
	        int sqlType = rs & 0xff;
	        RouteResultset rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
	        Assert.assertTrue("COMPANY".equals(tablename));
	        Assert.assertTrue(rrs.getNodes().length == nodeSize);
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



    @Test
    public void testTableMetaRead() throws Exception {
        final SchemaConfig schema = schemaMap.get("cndb");

        String sql = "desc offer";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = " desc cndb.offer";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = " desc cndb.offer col1";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer col1", rrs.getNodes()[0].getStatement());

        sql = "SHOW FULL COLUMNS FROM  offer  IN db_name WHERE true";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW FULL COLUMNS FROM offer WHERE true",
                rrs.getNodes()[0].getStatement());

        sql = "SHOW FULL COLUMNS FROM  db.offer  IN db_name WHERE true";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW FULL COLUMNS FROM offer WHERE true",
                rrs.getNodes()[0].getStatement());


        sql = "SHOW FULL TABLES FROM `TESTDB` WHERE Table_type != 'VIEW'";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("SHOW FULL TABLES WHERE Table_type != 'VIEW'", rrs.getNodes()[0].getStatement());

        sql = "SHOW INDEX  IN offer FROM  db_name";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW INDEX  FROM offer",
                rrs.getNodes()[0].getStatement());
    }
    
}
