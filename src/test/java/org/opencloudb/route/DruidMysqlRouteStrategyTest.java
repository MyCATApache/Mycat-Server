package org.opencloudb.route;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.Test;
import org.opencloudb.SimpleCachePool;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.server.parser.ServerParse;

import java.sql.SQLNonTransientException;
import java.util.*;

public class DruidMysqlRouteStrategyTest extends TestCase {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

    public DruidMysqlRouteStrategyTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
    }

    protected void setUp() throws Exception {
        // super.setUp();
        // schemaMap = CobarServer.getInstance().getConfig().getSchemas();
    }

//	public void testAlias() throws Exception {
//		String sql = "SELECT  UM.UserId , UM.MenuId ,SM.ParentId ,SM.FullName , SM.Description , SM.Img , SM.NavigateUrl ,SM.FormName ,SM.Target ,SM.IsUnfold FROM    Lever_SysMenu SM INNER JOIN ( SELECT UR.UserId AS UserId ,  RM.MenuId AS MenuId FROM   Lever_RoleMenu RM  INNER JOIN Lever_UserRole UR ON RM.RoleId = UR.RoleId  UNION SELECT UserId ,   MenuId FROM   Lever_UserMenu   UNION SELECT U.UserId ,      RM.MenuId   FROM   Lever_User U     LEFT JOIN Lever_RoleMenu RM ON U.RoleId = RM.RoleId    WHERE  U.UserId = '8d28533f-1762-4e79-b71f-64eb1a50cb8b' ) UM ON SM.MenuId = UM.MenuId   WHERE   UM.UserId = '8d28533f-1762-4e79-b71f-64eb1a50cb8b'  AND SM.Enabled = 1  ORDER BY SM.SortCode";
//		SchemaConfig schema = schemaMap.get("wdw");
//		RouteResultset rrs = routeStrategy.route(new SystemConfig(),schema, -1, sql, null,
//				null, cachePool);
//	}

    public void testRouteInsertShort() throws Exception {
        String sql = "inSErt into offer_detail (`offer_id`, gmt) values (123,now())";
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1l, rrs.getLimitSize());
        Assert.assertEquals("detail_dn15", rrs.getNodes()[0].getName());
        Assert.assertEquals(
                "inSErt into offer_detail (`offer_id`, gmt) values (123,now())",
                rrs.getNodes()[0].getStatement());

        sql = "inSErt into offer_detail ( gmt) values (now())";
        schema = schemaMap.get("cndb");
        try {
            rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        } catch (Exception e) {
            String msg = "bad insert sql (sharding column:";
            Assert.assertTrue(e.getMessage().contains(msg));
        }
        sql = "inSErt into offer_detail (offer_id, gmt) values (123,now())";
        schema = schemaMap.get("cndb");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1l, rrs.getLimitSize());
        Assert.assertEquals("detail_dn15", rrs.getNodes()[0].getName());
        Assert.assertEquals(
                "inSErt into offer_detail (offer_id, gmt) values (123,now())",
                rrs.getNodes()[0].getStatement());

        sql = "insert into offer(group_id,offer_id,member_id)values(234,123,'abc')";
        schema = schemaMap.get("cndb");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1l, rrs.getLimitSize());
        Assert.assertEquals("offer_dn12", rrs.getNodes()[0].getName());
        Assert.assertEquals(
                "insert into offer(group_id,offer_id,member_id)values(234,123,'abc')",
                rrs.getNodes()[0].getStatement());




        sql = "\n" +
                "  INSERT INTO \n" +
                "`offer` \n" +
                "(`asf`,member_id) \n" +
                "VALUES \n" +
                "(' the articles sfroms user selection ','abc')";
        schema = schemaMap.get("cndb");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);


    }

    public void testGlobalTableroute() throws Exception {
        String sql = null;
        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = null;
        // select of global table route to only one datanode defined
        sql = "select * from company where company.name like 'aaa'";
        schema = schemaMap.get("TESTDB");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        // query of global table only route to one datanode
        sql = "insert into company (id,name,level) values(111,'company1',3)";
        schema = schemaMap.get("TESTDB");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(3, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());

        // update of global table route to every datanode defined
        sql = "update company set name=name+aaa";
        schema = schemaMap.get("TESTDB");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(3, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        
     // delete of global table route to every datanode defined
        sql = "delete from company where id = 1";
        schema = schemaMap.get("TESTDB");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(3, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());

        // company is global table ,will route to differnt tables
        schema = schemaMap.get("TESTDB");
        sql = "select * from  company A where a.sharding_id=10001 union select * from  company B where B.sharding_id =10010";
        Set<String> nodeSet = new HashSet<String>();
        for (int i = 0; i < 10; i++) {
            rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
            Assert.assertEquals(false, rrs.isCacheAble());
            Assert.assertEquals(1, rrs.getNodes().length);
            nodeSet.add(rrs.getNodes()[0].getName());

        }
        Assert.assertEquals(true, nodeSet.size() > 1);

    }

    public void testMoreGlobalTableroute() throws Exception {
        String sql = null;
        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = null;
        // select of global table route to only one datanode defined
        sql = "select * from company,area where area.company_id=company.id ";
        schema = schemaMap.get("TESTDB");
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(true, rrs.isCacheAble());

    }

    public void testRouteMultiTables() throws Exception {
        // company is global table ,route to 3 datanode and ignored in route
        String sql = "select * from company,customer ,orders where customer.company_id=company.id and orders.customer_id=customer.id and company.name like 'aaa' limit 10";
        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(true, rrs.isCacheAble());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

    }

    public void testRouteCache() throws Exception {
        // select cache ID
        this.cachePool.putIfAbsent("TESTDB_EMPLOYEE", "88", "dn2");

        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select * from employee where id=88";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());//已经缓存了,不必再缓存了
        Assert.assertEquals(null, rrs.getPrimaryKey());
        Assert.assertEquals(-1, rrs.getLimitSize());
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        // select cache ID not found ,return all node and rrst not cached
        sql = "select * from employee where id=89";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("TESTDB_EMPLOYEE.ID", rrs.getPrimaryKey());
        Assert.assertEquals(-1, rrs.getLimitSize());

        // update cache ID found
        sql = "update employee  set name='aaa' where id=88";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(null, rrs.getPrimaryKey());
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        // delete cache ID found
        sql = "delete from  employee  where id=88";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

    }

    private static Map<String, RouteResultsetNode> getNodeMap(
            RouteResultset rrs, int expectSize) {
        RouteResultsetNode[] routeNodes = rrs.getNodes();
        Assert.assertEquals(expectSize, routeNodes.length);
        Map<String, RouteResultsetNode> nodeMap = new HashMap<String, RouteResultsetNode>(
                expectSize, 1);
        for (int i = 0; i < expectSize; i++) {
            RouteResultsetNode routeNode = routeNodes[i];
            nodeMap.put(routeNode.getName(), routeNode);
        }
        Assert.assertEquals(expectSize, nodeMap.size());
        return nodeMap;
    }

    private static interface NodeNameDeconstructor {
        public int getNodeIndex(String name);
    }

    private static class NodeNameAsserter implements NodeNameDeconstructor {
        private String[] expectNames;

        public NodeNameAsserter() {
        }

        public NodeNameAsserter(String... expectNames) {
            Assert.assertNotNull(expectNames);
            this.expectNames = expectNames;
        }

        protected void setNames(String[] expectNames) {
            Assert.assertNotNull(expectNames);
            this.expectNames = expectNames;
        }

        public void assertRouteNodeNames(Collection<String> nodeNames) {
            Assert.assertNotNull(nodeNames);
            Assert.assertEquals(expectNames.length, nodeNames.size());
            for (String name : expectNames) {
                Assert.assertTrue(nodeNames.contains(name));
            }
        }

        @Override
        public int getNodeIndex(String name) {
            for (int i = 0; i < expectNames.length; ++i) {
                if (name.equals(expectNames[i])) {
                    return i;
                }
            }
            throw new NoSuchElementException("route node " + name
                    + " dosn't exist!");
        }
    }

    private static class IndexedNodeNameAsserter extends NodeNameAsserter {
        /**
         * @param from included
         * @param to   excluded
         */
        public IndexedNodeNameAsserter(String prefix, int from, int to) {
            super();
            String[] names = new String[to - from];
            for (int i = 0; i < names.length; ++i) {
                names[i] = prefix + (i + from) ;
            }
            setNames(names);
        }
    }

    private static class RouteNodeAsserter {
        private NodeNameDeconstructor deconstructor;
        private SQLAsserter sqlAsserter;

        public RouteNodeAsserter(NodeNameDeconstructor deconstructor,
                                 SQLAsserter sqlAsserter) {
            this.deconstructor = deconstructor;
            this.sqlAsserter = sqlAsserter;
        }

        public void assertNode(RouteResultsetNode node) throws Exception {
            int nodeIndex = deconstructor.getNodeIndex(node.getName());
            sqlAsserter.assertSQL(node.getStatement(), nodeIndex);
        }
    }

    private static interface SQLAsserter {
        public void assertSQL(String sql, int nodeIndex) throws Exception;
    }

    private static class SimpleSQLAsserter implements SQLAsserter {
        private Map<Integer, Set<String>> map = new HashMap<Integer, Set<String>>();

        public SimpleSQLAsserter addExpectSQL(int nodeIndex, String sql) {
            Set<String> set = map.get(nodeIndex);
            if (set == null) {
                set = new HashSet<String>();
                map.put(nodeIndex, set);
            }
            set.add(sql);
            return this;
        }

        @Override
        public void assertSQL(String sql, int nodeIndex) throws Exception {
            Assert.assertNotNull(map.get(nodeIndex));
            Assert.assertTrue(map.get(nodeIndex).contains(sql));
        }
    }

    public void testroute() throws Exception {
        SchemaConfig schema = schemaMap.get("cndb");

        String sql = "select * from independent where member='abc'";

        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Map<String, RouteResultsetNode> nodeMap = getNodeMap(rrs, 128);
        IndexedNodeNameAsserter nameAsserter = new IndexedNodeNameAsserter(
                "independent_dn", 0, 128);
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        SimpleSQLAsserter sqlAsserter = new SimpleSQLAsserter();
        for (int i = 0; i < 128; ++i) {
            sqlAsserter.addExpectSQL(i,
                    "select * from independent where member='abc'");
        }
        RouteNodeAsserter asserter = new RouteNodeAsserter(nameAsserter,
                sqlAsserter);
        for (RouteResultsetNode node : nodeMap.values()) {
            asserter.assertNode(node);
        }

        // include database schema ,should remove
        sql = "select * from cndb.independent A  where a.member='abc'";
        schema = schemaMap.get("cndb");
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        nodeMap = getNodeMap(rrs, 128);
        nameAsserter = new IndexedNodeNameAsserter("independent_dn", 0, 128);
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        sqlAsserter = new SimpleSQLAsserter();
        for (int i = 0; i < 128; ++i) {
            sqlAsserter.addExpectSQL(i,
                    "select * from independent A  where a.member='abc'");
        }
        asserter = new RouteNodeAsserter(nameAsserter, sqlAsserter);
        for (RouteResultsetNode node : nodeMap.values()) {
            asserter.assertNode(node);
        }

    }

    public void testERroute() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "insert into orders (id,name,customer_id) values(1,'testonly',1)";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());

        sql = "insert into orders (id,name,customer_id) values(1,'testonly',2000001)";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        // can't update join key
        sql = "update orders set id=1 ,name='aaa' , customer_id=2000001";
        String err = null;
        try {
            rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        } catch (SQLNonTransientException e) {
            err = e.getMessage();
        }
        Assert.assertEquals(
                true,
                err.startsWith("Parent relevant column can't be updated ORDERS->CUSTOMER_ID"));

        // route by parent rule ,update sql
        sql = "update orders set id=1 ,name='aaa' where customer_id=2000001";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        // route by parent rule but can't find datanode
        sql = "update orders set id=1 ,name='aaa' where customer_id=-1";
        try {
            rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        } catch (Exception e) {
            err = e.getMessage();
        }
        Assert.assertEquals(true,
                err.startsWith("can't find datanode for sharding column:"));

        // route by parent rule ,select sql
        sql = "select * from orders  where customer_id=2000001";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        // route by parent rule ,delete sql
        sql = "delete from orders  where customer_id=2000001";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals("dn2", rrs.getNodes()[0].getName());

        //test alias in column
        sql = "select name as order_name from  orders order by order_name limit 10,5";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        MySqlStatementParser parser = new MySqlStatementParser("SELECT name AS order_name FROM orders ORDER BY order_name LIMIT 0,15");
        SQLStatement statement = parser.parseStatement();

//		Assert.assertEquals(sql, rrs.getNodes()[0].getStatement());
    }

    public void testDuplicatePartitionKey() throws Exception {
        String sql = null;
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = null;

        sql = "select * from cndb.offer where (offer_id, group_id ) In (123,234)";
        schema = schemaMap.get("cndb");
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Assert.assertEquals(-1l, rrs.getLimitSize());
        Assert.assertEquals(128, rrs.getNodes().length);
        for (int i = 0; i < 128; i++) {
//            Assert.assertEquals("offer_dn" + i ,
//                    rrs.getNodes()[i].getName());//node的排序有变化，所以此处不强求
            Assert.assertEquals(
                    "select * from offer where (offer_id, group_id ) In (123,234)",
                    rrs.getNodes()[i].getStatement());
        }

        sql = "SELECT * FROM offer WHERE FALSE OR offer_id = 123 AND member_id = 123 OR member_id = 123 AND member_id = 234 OR member_id = 123 AND member_id = 345 OR member_id = 123 AND member_id = 456 OR offer_id = 234 AND group_id = 123 OR offer_id = 234 AND group_id = 234 OR offer_id = 234 AND group_id = 345 OR offer_id = 234 AND group_id = 456 OR offer_id = 345 AND group_id = 123 OR offer_id = 345 AND group_id = 234 OR offer_id = 345 AND group_id = 345 OR offer_id = 345 AND group_id = 456 OR offer_id = 456 AND group_id = 123 OR offer_id = 456 AND group_id = 234 OR offer_id = 456 AND group_id = 345 OR offer_id = 456 AND group_id = 456";
        schema = schemaMap.get("cndb");
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        getNodeMap(rrs, 128);

        sql = "select * from  offer where false"
                + " or offer_id=123 and group_id=123"
                + " or group_id=123 and offer_id=234"
                + " or offer_id=123 and group_id=345"
                + " or offer_id=123 and group_id=456  ";
        schema = schemaMap.get("cndb");
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Assert.assertEquals(-1l, rrs.getLimitSize());

    }

    public void testAddLimitToSQL() throws Exception {
        final SchemaConfig schema = schemaMap.get("TESTDB");

        String sql = null;
        RouteResultset rrs = null;

        sql = "select * from orders";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Map<String, RouteResultsetNode> nodeMap = getNodeMap(rrs, 2);
        NodeNameAsserter nameAsserter = new NodeNameAsserter("dn2",
                "dn1");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        Assert.assertEquals(schema.getDefaultMaxLimit(), rrs.getLimitSize());
//		Assert.assertEquals("SELECT * FROM orders LIMIT 100", rrs.getNodes()[0].getStatement());
        MySqlStatementParser parser = new MySqlStatementParser("SELECT * FROM orders LIMIT 100");
        SQLStatement statement = parser.parseStatement();
        Assert.assertEquals(statement.toString(), rrs.getNodes()[0].getStatement());


        sql = "select * from goods";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(schema.getDefaultMaxLimit(), rrs.getLimitSize());
//		Assert.assertEquals("select * from goods", rrs.getNodes()[0].getStatement());
        parser = new MySqlStatementParser("SELECT * FROM goods LIMIT 100");
        statement = parser.parseStatement();
        Assert.assertEquals(statement.toString(), rrs.getNodes()[0].getStatement());


        sql = "select * from goods limit 2 ,3";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(1, rrs.getNodes().length);
//		Assert.assertEquals(-1, rrs.getLimitSize());
        Assert.assertEquals("select * from goods limit 2 ,3", rrs.getNodes()[0].getStatement());


        sql = "select * from notpartionTable limit 2 ,3";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(3, rrs.getLimitSize());
        Assert.assertEquals("select * from notpartionTable limit 2 ,3", rrs.getNodes()[0].getStatement());

    }


    public void testModifySQLLimit() throws Exception {
        final SchemaConfig schema = schemaMap.get("TESTDB");

        String sql = null;
        RouteResultset rrs = null;
        //SQL span multi datanode 
        sql = "select * from orders limit 2,3";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        Map<String, RouteResultsetNode> nodeMap = getNodeMap(rrs, 2);
        NodeNameAsserter nameAsserter = new NodeNameAsserter("dn2",
                "dn1");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        Assert.assertEquals(3, rrs.getLimitSize());


        MySqlStatementParser parser = new MySqlStatementParser("SELECT * FROM orders LIMIT 0,5");
        SQLStatement statement = parser.parseStatement();

        Assert.assertEquals(statement.toString(), rrs.getNodes()[0].getStatement());

        //SQL  not span multi datanode
        sql = "select * from customer where id=10000 limit 2,3";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        nodeMap = getNodeMap(rrs, 1);
        nameAsserter = new NodeNameAsserter("dn1");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        Assert.assertEquals(3, rrs.getLimitSize());
        Assert.assertEquals("select * from customer where id=10000 limit 2,3", rrs.getNodes()[0].getStatement());


    }

    public void testGroupLimit() throws Exception {
        final SchemaConfig schema = schemaMap.get("cndb");

        String sql = null;
        RouteResultset rrs = null;

        sql = "select count(*) from (select * from(select * from offer_detail where offer_id='123' or offer_id='234' limit 88)offer  where offer.member_id='abc' limit 60) w "
                + " where w.member_id ='pavarotti17' limit 99";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        // Assert.assertEquals(88L, rrs.getLimitSize());
        // Assert.assertEquals(RouteResultset.SUM_FLAG, rrs.getFlag());
        Map<String, RouteResultsetNode> nodeMap = getNodeMap(rrs, 2);
        NodeNameAsserter nameAsserter = new NodeNameAsserter("detail_dn29",
                "detail_dn15");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());

        sql = "select count(*) from (select * from(select max(id) from offer_detail where offer_id='123' or offer_id='234' limit 88)offer  where offer.member_id='abc' limit 60) w "
                + " where w.member_id ='pavarotti17' limit 99";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        nodeMap = getNodeMap(rrs, 2);
        nameAsserter = new NodeNameAsserter("detail_dn29", "detail_dn15");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());

        sql = "select * from (select * from(select max(id) from offer_detail where offer_id='123' or offer_id='234' limit 88)offer  where offer.member_id='abc' limit 60) w "
                + " where w.member_id ='pavarotti17' limit 99";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        nodeMap = getNodeMap(rrs, 2);
        nameAsserter = new NodeNameAsserter("detail_dn29", "detail_dn15");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());

        sql = "select * from (select count(*) from(select * from offer_detail where offer_id='123' or offer_id='234' limit 88)offer  where offer.member_id='abc' limit 60) w "
                + " where w.member_id ='pavarotti17' limit 99";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(true, rrs.isCacheAble());
        // Assert.assertEquals(88L, rrs.getLimitSize());
        // Assert.assertEquals(RouteResultset.SUM_FLAG, rrs.getFlag());
        nodeMap = getNodeMap(rrs, 2);
        nameAsserter = new NodeNameAsserter("detail_dn29", "detail_dn15");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());

    }

    public void testTableMetaRead() throws Exception {
        final SchemaConfig schema = schemaMap.get("cndb");

        String sql = " desc offer";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = "desc cndb.offer";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = "desc cndb.offer col1";
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

        sql = "SHOW TABLES from db_name like 'solo'";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Map<String, RouteResultsetNode> nodeMap = getNodeMap(rrs, 3);
        NodeNameAsserter nameAsserter = new NodeNameAsserter("detail_dn0",
                "offer_dn0", "independent_dn0");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        SimpleSQLAsserter sqlAsserter = new SimpleSQLAsserter();
        sqlAsserter.addExpectSQL(0, "SHOW TABLES like 'solo'")
                .addExpectSQL(1, "SHOW TABLES like 'solo'")
                .addExpectSQL(2, "SHOW TABLES like 'solo'")
                .addExpectSQL(3, "SHOW TABLES like 'solo'");
        RouteNodeAsserter asserter = new RouteNodeAsserter(nameAsserter,
                sqlAsserter);
        for (RouteResultsetNode node : nodeMap.values()) {
            asserter.assertNode(node);
        }

        sql = "SHOW TABLES in db_name ";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        nodeMap = getNodeMap(rrs, 3);
        nameAsserter = new NodeNameAsserter("detail_dn0", "offer_dn0",
                "independent_dn0");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        sqlAsserter = new SimpleSQLAsserter();
        sqlAsserter.addExpectSQL(0, "SHOW TABLES")
                .addExpectSQL(1, "SHOW TABLES").addExpectSQL(2, "SHOW TABLES")
                .addExpectSQL(3, "SHOW TABLES");
        asserter = new RouteNodeAsserter(nameAsserter, sqlAsserter);
        for (RouteResultsetNode node : nodeMap.values()) {
            asserter.assertNode(node);
        }

        sql = "SHOW TABLeS ";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        nodeMap = getNodeMap(rrs, 3);
        nameAsserter = new NodeNameAsserter("offer_dn0", "detail_dn0",
                "independent_dn0");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        sqlAsserter = new SimpleSQLAsserter();
        sqlAsserter.addExpectSQL(0, "SHOW TABLeS ")
                .addExpectSQL(1, "SHOW TABLeS ").addExpectSQL(2, "SHOW TABLeS ");
        asserter = new RouteNodeAsserter(nameAsserter, sqlAsserter);
        for (RouteResultsetNode node : nodeMap.values()) {
            asserter.assertNode(node);
        }
    }

    public void testConfigSchema() throws Exception {
        try {
            SchemaConfig schema = schemaMap.get("config");
            String sql = "select * from offer where offer_id=1";
            routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
            Assert.assertFalse(true);
        } catch (Exception e) {
            Assert.assertEquals("route rule for table OFFER is required: select * from offer where offer_id=1", e.getMessage());
        }
        try {
            SchemaConfig schema = schemaMap.get("config");
            String sql = "select * from offer where col11111=1";
            routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
            Assert.assertFalse(true);
        } catch (Exception e) {
        }
        try {
            SchemaConfig schema = schemaMap.get("config");
            String sql = "select * from offer ";
            routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
            Assert.assertFalse(true);
        } catch (Exception e) {
        }
    }

    public void testIgnoreSchema() throws Exception {
        SchemaConfig schema = schemaMap.get("ignoreSchemaTest");
        String sql = "select * from offer where offer_id=1";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("cndb_dn", rrs.getNodes()[0].getName());
        Assert.assertEquals(sql, rrs.getNodes()[0].getStatement());
        sql = "select * from ignoreSchemaTest.offer1 where ignoreSchemaTest.offer1.offer_id=1";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("select * from offer1 where offer1.offer_id=1",
                rrs.getNodes()[0].getStatement());
        sql = "select * from ignoreSchemaTest2.offer where ignoreSchemaTest2.offer.offer_id=1";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(sql, rrs.getNodes()[0].getStatement(), sql);
        sql = "select * from ignoreSchemaTest2.offer a,offer b  where ignoreSchemaTest2.offer.offer_id=1";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(
                "select * from ignoreSchemaTest2.offer a,offer b  where ignoreSchemaTest2.offer.offer_id=1",
                rrs.getNodes()[0].getStatement());

    }

    public void testNonPartitionSQL() throws Exception {

        SchemaConfig schema = schemaMap.get("cndb");
        String sql = null;
        RouteResultset rrs = null;

        schema = schemaMap.get("dubbo2");
        sql = "SHOW TABLES from db_name like 'solo'";
        rrs = routeStrategy.route(new SystemConfig(), schema, 9, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW TABLES like 'solo'",
                rrs.getNodes()[0].getStatement());

        schema = schemaMap.get("dubbo");
        sql = "SHOW TABLES from db_name like 'solo'";
        rrs = routeStrategy.route(new SystemConfig(), schema, 9, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dubbo_dn", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW TABLES like 'solo'",
                rrs.getNodes()[0].getStatement());



        sql = "desc cndb.offer";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dubbo_dn", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc cndb.offer", rrs.getNodes()[0].getStatement());

        schema = schemaMap.get("cndb");
        sql = "SHOW fulL TaBLES from db_name like 'solo'";
        rrs = routeStrategy.route(new SystemConfig(), schema, 9, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Map<String, RouteResultsetNode> nodeMap = getNodeMap(rrs, 3);
        NodeNameAsserter nameAsserter = new NodeNameAsserter("detail_dn0",
                "offer_dn0", "independent_dn0");
        nameAsserter.assertRouteNodeNames(nodeMap.keySet());
        SimpleSQLAsserter sqlAsserter = new SimpleSQLAsserter();
        sqlAsserter.addExpectSQL(0, "SHOW FULL TABLES like 'solo'")
                .addExpectSQL(1, "SHOW FULL TABLES like 'solo'")
                .addExpectSQL(2, "SHOW FULL TABLES like 'solo'")
                .addExpectSQL(3, "SHOW FULL TABLES like 'solo'");
        RouteNodeAsserter asserter = new RouteNodeAsserter(nameAsserter,
                sqlAsserter);
        for (RouteResultsetNode node : nodeMap.values()) {
            asserter.assertNode(node);
        }
    }

    public void testGlobalTableSingleNodeLimit() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select * from globalsn";
        RouteResultset rrs = null;
        rrs = routeStrategy.route(new SystemConfig(), schema,
                ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(100L, rrs.getLimitSize());
    }

    /**
     * select 1
     * select 1 union all select 2
     *
     * @throws Exception
     */
    public void testSelectNoTable() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select 1";
        RouteResultset rrs = null;
        rrs = routeStrategy.route(new SystemConfig(), schema,
                ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);


        sql = "select 1 union select 2";
        rrs = routeStrategy.route(new SystemConfig(), schema,
                ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
    }

    /**
     * 支持insert into ... values (),()...
     * 不支持insert into ... select...
     *
     * @throws Exception
     */
    public void testBatchInsert() throws Exception {

        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = null;
        //不支持childtable 批量插入
        String sql = "insert into orders (id,name,customer_id) values(1,'testonly',1),(2,'testonly',2000001)";
        try {
            rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                    cachePool);
        } catch (Exception e) {
            Assert.assertEquals("ChildTable multi insert not provided", e.getMessage());
        }

        sql = "insert into employee (id,name,customer_id) select id,name,customer_id from customer";
        try {
            rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                    cachePool);
        } catch (Exception e) {
            Assert.assertEquals("TODO:insert into .... select .... not supported!", e.getMessage());
        }

        //分片表批量插入正常 employee
        sql = "insert into employee (id,name,sharding_id) values(1,'testonly',10000),(2,'testonly',10010)";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());
        String node1Sql = formatSql("insert into employee (id,name,sharding_id) values(1,'testonly',10000)");
        String node2Sql = formatSql("insert into employee (id,name,sharding_id) values(2,'testonly',10010)");
        RouteResultsetNode[] nodes = rrs.getNodes();
        Assert.assertEquals(node1Sql, nodes[0].getStatement());
        Assert.assertEquals(node2Sql, nodes[1].getStatement());
    }

    /**
     * insert ... on duplicate key ... update...
     *
     * @throws Exception
     */
    public void testInsertOnDuplicateKey() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "insert into employee (id,name,sharding_id) values(1,'testonly',10000) on duplicate key update name='nihao'";
        RouteResultset rrs = null;
        rrs = routeStrategy.route(new SystemConfig(), schema,
                ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());

        //insert ... on duplicate key ... update col1 = VALUES(col1),col2 = VALUES(col2)
        sql = "insert into employee (id,name,sharding_id) values(1,'testonly',10000) " +
                "on duplicate key update name=VALUES(name),id = VALUES(id)";
        rrs = routeStrategy.route(new SystemConfig(), schema,
                ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());

        //insert ... on duplicate key ,sharding key can't be updated
        sql = "insert into employee (id,name,sharding_id) values(1,'testonly',10000) " +
                "on duplicate key update name=VALUES(name),id = VALUES(id),sharding_id = VALUES(sharding_id)";

        try {
            rrs = routeStrategy.route(new SystemConfig(), schema,
                    ServerParse.SELECT, sql, null, null, cachePool);
        } catch (Exception e) {
            Assert.assertEquals("Sharding column can't be updated: EMPLOYEE -> SHARDING_ID", e.getMessage());
        }


    }

    /**
     * 测试函数COUNT
     *
     * @throws Exception
     */
    @Test
    public void testAggregateExpr() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select id, name, count(name) from employee group by name;";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getMergeCols().containsKey("COUNT2"));

        sql = "select id, name, count(name) as c from employee group by name;";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getMergeCols().containsKey("c"));

        sql = "select id, name, count(name) c from employee group by name;";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getMergeCols().containsKey("c"));
    }
    
    /**
     * 测试between语句的路由
     *
     * @throws Exception
     */
    @Test
    public void testBetweenExpr() throws Exception {
//    	0-200M=0
//    	200M1-400M=1
//    	400M1-600M=2
//    	600M1-800M=3
//    	800M1-1000M=4
    	
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select * from customer where id between 1 and 5;";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes().length == 1);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn1"));

        sql = "select * from customer where id between 1 and 2000001;";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes().length == 2);
        
        sql = "select * from customer where id between 2000001 and 3000001;";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes().length == 1);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn2"));
        
        sql = "delete from customer where id between 2000001 and 3000001;";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes().length == 1);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn2"));
        
        sql = "update customer set name='newName' where id between 2000001 and 3000001;";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes().length == 1);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn2"));
        
    }
    
    /**
     * 测试or语句的路由
     *
     * @throws Exception
     */
    @Test
    public void testOr() throws Exception {
//    	0-200M=0
//    	200M1-400M=1
//    	400M1-600M=2
//    	600M1-800M=3
//    	800M1-1000M=4
    	
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select * from customer where sharding_id=10000 or 1=1;";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes().length == 2);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn1"));
        Assert.assertTrue(rrs.getNodes()[1].getName().equals("dn2"));

        sql = "select * from customer where sharding_id = 10000 or sharding_id = 10010";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn1"));
        Assert.assertTrue(rrs.getNodes()[1].getName().equals("dn2"));
        
        sql = "select * from customer where sharding_id = 10000 or user_id = 'wangwu'";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn1"));
        Assert.assertTrue(rrs.getNodes()[1].getName().equals("dn2"));
    }
    
    /**
     * 测试父子表，查询子表的语句路由到多个节点
     * @throws Exception
     */
    @Test
    public void testERRouteMutiNode() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select * from orders where customer_id in(1,2000001);";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
        Assert.assertTrue(rrs.getNodes().length == 2);
        Assert.assertTrue(rrs.getNodes()[0].getName().equals("dn1"));
        Assert.assertTrue(rrs.getNodes()[1].getName().equals("dn2"));
    }
    
    /**
     * 测试多层or语句
     *
     * @throws Exception
     */
    @Test
    public void testMultiLevelOr() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select id from travelrecord "
        			+ " where id = 1 and ( fee=3 or days=5 or (traveldate = '2015-05-04 00:00:07.375' "
        			+ " and (user_id=2 or fee=days or fee = 0))) and name = 'zhangsan'" ;
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);

        Assert.assertTrue(rrs.getNodes().length == 1);
        
	    sql = "select id from travelrecord "
	    			+ " where id = 1 and ( fee=3 or days=5 or (traveldate = '2015-05-04 00:00:07.375' "
	    			+ " and (user_id=2 or fee=days or fee = 0))) and name = 'zhangsan' or id = 2000001" ;
	    rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
	
	    Assert.assertTrue(rrs.getNodes().length == 2);
	    
	    sql = "select id from travelrecord "
    			+ " where id = 1 and ( fee=3 or days=5 or (traveldate = '2015-05-04 00:00:07.375' "
    			+ " and (user_id=2 or fee=days or fee = 0))) and name = 'zhangsan' or id = 2000001 or id = 4000001" ;
	    rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
	
	    Assert.assertTrue(rrs.getNodes().length == 3);
    }
    
    /**
     * 测试 global table 的or语句
     * 
     *
     * @throws Exception
     */
    @Test
    public void testGlobalTableOr() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        String sql = "select id from company where 1 = 1 and name ='company1' or name = 'company2'" ;
        for(int i = 0; i < 20; i++) {
        	RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SELECT, sql, null, null, cachePool);
            Assert.assertTrue(rrs.getNodes().length == 1);
        }
    }
    
    /**
     * 测试别名路由
     *
     * @throws Exception
     */
    public void testAlias() throws Exception {

        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = null;
        //不支持childtable 批量插入
        //update 全局表
        String sql = "update company a set name = '' where a.id = 1;";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                    cachePool);

        Assert.assertEquals(3, rrs.getNodes().length);

        //update带别名时的路由
        sql = "update travelrecord a set name = '' where a.id = 1;";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                    cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        
        //别名大小写路由
        sql = "select * from travelrecord A where a.id = 1;";
        rrs = routeStrategy.route(new SystemConfig(), schema, 1, sql, null, null,
                    cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
    }
    
    private String formatSql(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        return stmt.toString();
    }


}
