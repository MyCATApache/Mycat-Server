package org.opencloudb.config.loader.zookeeper;

import com.google.common.base.Strings;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class ZookeeperLoader {
    private static final String ZK_CONFIG_FILE_NAME = "/myid.properties";
    public static final String NODE_KEY = "node";
    public static final String CLUSTER_KEY = "cluster";
    public static final String MYSQLGROUP_KEY = "mysqlGroup";
    public static final String MYSQLS_KEY = "mysqls";

    protected String zkURl;

    public JSONObject buildConfig() throws Exception {
        Properties properties = loadZkConfig();

        CuratorFramework curatorFramework =
            buildConnection((zkURl == null) ? properties.getProperty("zkURL") : zkURl);

        return takeConfig(properties.getProperty("myid"), curatorFramework);
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

    private Properties loadZkConfig() {
        Properties pros = new Properties();

        try (InputStream configIS = ZookeeperLoader.class
            .getResourceAsStream(ZK_CONFIG_FILE_NAME)) {
            pros.load(configIS);
        } catch (IOException e) {
            throw new RuntimeException("can't find myid properties file : " + ZK_CONFIG_FILE_NAME);
        }

        //validate
        String zkURL = pros.getProperty("zkURL");
        String myid = pros.getProperty("myid");

        if (Strings.isNullOrEmpty(zkURL) || Strings.isNullOrEmpty(myid)) {
            throw new RuntimeException("zkURL and myid must be not null or empty!");
        }

        return pros;
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
