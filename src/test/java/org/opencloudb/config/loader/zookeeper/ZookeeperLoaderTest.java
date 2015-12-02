package org.opencloudb.config.loader.zookeeper;

import demo.catlets.ZkCreate;
import org.apache.curator.test.TestingServer;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by lion on 12/6/15.
 */
public class ZookeeperLoaderTest {

    private static TestingServer testingServer;

    @BeforeClass public static void setUpZookeeper() throws Exception {
        testingServer = new TestingServer(true);
        ZkCreate.main(new String[] {"/zk-create-test.yaml", testingServer.getConnectString()});
    }

    @AfterClass public static void tearDown() throws IOException {
        testingServer.close();
    }

    @Test public void testBuildConfig() throws Exception {

        ZookeeperLoader loader = new ZookeeperLoader();
        loader.setZkURl(testingServer.getConnectString());

        JSONObject jsonObject = loader.buildConfig();

        JSONObject node = jsonObject.getJSONObject(ZookeeperLoader.NODE_KEY);
        JSONObject mysqlGroup = jsonObject.getJSONObject(ZookeeperLoader.MYSQLGROUP_KEY);
        JSONObject mysqls = jsonObject.getJSONObject(ZookeeperLoader.MYSQLS_KEY);
        JSONObject cluster = jsonObject.getJSONObject(ZookeeperLoader.CLUSTER_KEY);

        String expectNode = "{\"weigth\":1,\"name\":\"mycat_fz_01\",\"leader\":1,\"state\":\"red\","
            + "\"systemParams\":{\"serverport\":8066,\"sequncehandlertype\":1,"
            + "\"defaultsqlparser\":\"druidparser\"},\"hostname\":\"fz_vm1\","
            + "\"cluster\":\"mycat-cluster-1\",\"zone\":\"fz\"}";
        assertThat(node.toString(), is(expectNode));

        String expectMysqlGroup =
            "{\"mysql_rep_1\":{\"cur-write-server\":\"mysqlId2\",\"servers\":[\"mysqlId1\","
                + "\"mysqlId2\",\"mysqlId3\"],\"repType\":0,\"name\":\"mysql_rep_1\","
                + "\"auto-write-switch\":true,\"heartbeatSQL\":\"select user()\",\"zone\":\"bj\"}}";

        assertThat(mysqlGroup.toString(), is(expectMysqlGroup));

        String expectMysql =
            "{\"mysql_1\":{\"port\":3366,\"hostId\":\"host\",\"password\":\"mysql\","
                + "\"user\":\"mysql\",\"zone\":\"bj\",\"ip\":\"192.168.8.2\"}}";
        assertThat(mysqls.toString(), is(expectMysql));

        String expectCluster =
            "{\"schema\":{\"TESTDB\":{\"hotnews\":{\"ruleName\":\"sharding-by-mod\","
                + "\"primaryKey\":\"ID\",\"name\":\"hotnews\",\"datanode\":\"dn1,dn2,dn3\"},"
                + "\"company\":{\"primaryKey\":\"ID\",\"name\":\"company\","
                + "\"datanode\":\"dn1,dn2,dn3\",\"type\":\"1  //全局表为 1\"},"
                + "\"goods\":{\"primaryKey\":\"ID\",\"name\":\"goods\",\"datanode\":\"dn1,dn2\","
                + "\"type\":\"1  //全局表为 1\"},"
                + "\"offer1\":{\"ruleName\":\"sharding-by-RangeDateHash\",\"primaryKey\":\"id\","
                + "\"name\":\"offer1\",\"datanode\":\"offer_dn$1-36\"},"
                + "\"offer\":{\"ruleName\":\"auto-sharding-rang-mod\",\"primaryKey\":\"id\","
                + "\"name\":\"offer\",\"datanode\":\"offer_dn$1-20\"},"
                + "\"customer\":{\"customer_addr\":{\"name\":\"customer_addr\","
                + "\"joinkey\":\"customer_id\",\"parentkey\":\"ID\"},"
                + "\"ruleName\":\"sharding-by-enum\",\"primaryKey\":\"ID\","
                + "\"name\":\"customer\",\"orders\":{\"order_items\":{\"name\":\"order_items\","
                + "\"joinkey\":\"order_id\",\"parentkey\":\"ID\"}},\"datanode\":\"dn1,dn2\"},"
                + "\"employee\":{\"ruleName\":\"sharding-by-enum\",\"primaryKey\":\"ID\","
                + "\"name\":\"employee\",\"datanode\":\"dn1,dn2\"},"
                + "\"travelrecord\":{\"ruleName\":\"auto-sharding-long\",\"name\":\"travelrecord\","
                + "\"datanode\":\"dn1,dn2,dn3\"}}},\"datahost\":{\"localhost1\":{\"switchType\":1,"
                + "\"balance\":0,\"dbtype\":\"mysql\",\"mysqlGroup\":\"mysql_rep_1\","
                + "\"writetype\":0,\"slaveThreshold\":100,\"name\":\"localhost1\","
                + "\"dbDriver\":\"native\",\"mincon\":10,\"maxcon\":1000,"
                + "\"heartbeatSQL\":\"select user()\"}},"
                + "\"sequence\":{\"sequence-1\":{\"config\":{\"current_value\":100000,"
                + "\"increament\":100},\"sequence-mapping\":{\"T_NODE\":0}},"
                + "\"sequence-0\":{\"type\":\"file\"},\"sequence-3\":{\"current_value\":100000,"
                + "\"increament\":100},\"sequence-2\":{\"centerid\":2,\"workid\":1}},"
                + "\"rule\":{\"sharding-by-enum\":{\"config\":{\"10000\":0,\"10010\":1}},"
                + "\"auto-sharding-long\":{\"config\":{\"2000001-4000000\":1,\"0-2000000\":0,"
                + "\"4000001-8000000\":2}},\"sharding-by-hour\":{\"splitOneDay\":24,"
                + "\"name\":\"sharding-by-hour\",\"column\":\"createTime\","
                + "\"functionName\":\"io.mycat.route.function.LatestMonthPartion\"},"
                + "\"auto-sharding-rang-mod\":{\"sPartionDay\":3,\"groupPartionSize\":6,"
                + "\"name\":\"sharding-by-RangeDateHash\",\"column\":\"create_time\","
                + "\"sBeginDate\":\"2014-01-01 00:00:00\",\"dateFormat\":\"yyyy-MM-dd HH:mm:ss\","
                + "\"functionName\":\"io.mycat.route.function.PartitionByRangeDateHash\"},"
                + "\"sharding-by-mod\":{\"count\":3,\"name\":\"sharding-by-mod\",\"column\":\"id\","
                + "\"functionName\":\"io.mycat.route.function.PartitionByMod\"}},"
                + "\"datanode\":{\"offer_dn$0-127\":{\"name\":\"offer_dn$0-127\","
                + "\"dataHost\":\"localhost1\",\"database\":\"db1$0-127\"},"
                + "\"dn2\":{\"name\":\"dn2\",\"dataHost\":\"localhost1\","
                + "\"database\":\"db2\"},\"dn3\":{\"name\":\"dn3\",\"dataHost\":\"localhost1\","
                + "\"database\":\"db3\"},\"dn1\":{\"name\":\"dn1\",\"dataHost\":\"localhost1\","
                + "\"database\":\"db1\"}},\"user\":{\"test\":{\"schemas\":[\"testdb\"],"
                + "\"readOnly\":true,\"name\":\"test\",\"password\":\"admin\"},"
                + "\"mycat\":{\"schemas\":[\"testdb\"],\"readOnly\":false,\"name\":\"mycat\","
                + "\"password\":\"admin\"}},\"blockSQLs\":{\"sql3\":{\"name\":\"sql3\"},"
                + "\"sql2\":{\"name\":\"sql2\"},\"sql1\":{\"name\":\"sql1\"}}}";

        assertThat(cluster.toString(), is(expectCluster));
    }
}
