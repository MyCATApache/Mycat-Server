package org.opencloudb.config.loader.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.json.JSONObject;
import org.opencloudb.config.loader.zookeeper.loader.ClusterLoader;
import org.opencloudb.config.loader.zookeeper.loader.MysqlGroupLoader;
import org.opencloudb.config.loader.zookeeper.loader.MysqlsLoader;
import org.opencloudb.config.loader.zookeeper.loader.NodesLoader;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class ZookeeperLoader {
    public static final String NODE_KEY = "node";
    public static final String CLUSTER_KEY = "cluster";
    public static final String MYSQLGROUP_KEY = "mysqlGroup";
    public static final String MYSQLS_KEY = "mysqls";

    public static final String ZKURL_KEY = "zkURL";
    public static final String MYID_KEY = "myid";

    protected String zkURl;

    public JSONObject loadConfig(Properties properties) throws Exception {
        CuratorFramework curatorFramework =
            buildConnection((zkURl == null) ? properties.getProperty(ZKURL_KEY) : zkURl);

        return takeConfig(properties.getProperty(MYID_KEY), curatorFramework);
    }

    private JSONObject takeConfig(String myid, CuratorFramework curatorFramework) throws Exception {
        /**
         * take nodes data from path /mycat/mycat-nodes/{myid} in zookeeper
         */
        JSONObject node = new NodesLoader(curatorFramework).takeConfig(myid);

        /**
         * take cluster data from path /mycat/mycat-cluster/{myCluster} in zookeeper
         */
        JSONObject cluster =
            new ClusterLoader(curatorFramework).takeConfig(node.getString("cluster"));

        /**
         * take mysqlgroup data from path /mycat/mycat-mysqlgroup in zookeeper
         */
        JSONObject mysqlGroup = new MysqlGroupLoader(curatorFramework).takeConfig(null);

        /**
         * take mysql data from path /mycat/mycat-mysqls in zookeeper
         */
        JSONObject mysqls = new MysqlsLoader(curatorFramework).takeConfig(null);

        JSONObject composeJson = new JSONObject();
        composeJson.put(NODE_KEY, node);
        composeJson.put(CLUSTER_KEY, cluster);
        composeJson.put(MYSQLGROUP_KEY, mysqlGroup);
        composeJson.put(MYSQLS_KEY, mysqls);
        return composeJson;
    }

    public void setZkURl(String zkURl) {
        this.zkURl = zkURl;
    }

    private CuratorFramework buildConnection(String url) {
        CuratorFramework curatorFramework =
            CuratorFrameworkFactory.newClient(url, new ExponentialBackoffRetry(100, 6));

        //start connection
        curatorFramework.start();
        //wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework.usingNamespace("mycat");
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        //fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }
}
