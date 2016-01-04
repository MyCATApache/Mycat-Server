package demo.catlets;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by v1.lion on 2015/10/7.
 */
public class ZkCreate {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkCreate.class);
    private static final String CONFIG_URL_KEY = "zkURL";
    private static final String MYCAT_CLUSTER_KEY = "mycat-cluster";
    private static final String MYCAT_ZONE_KEY = "mycat-zones";
    private static final String MYCAT_NODES_KEY = "mycat-nodes";
    private static final String MYCAT_HOST_KEY = "mycat-hosts";
    private static final String MYCAT_MYSQLS_KEY = "mycat-mysqls";
    private static final String MYCAT_MYSQL_GROUP_KEY = "mycat-mysqlgroup";
    private static final String MYCAT_LBS = "mycat-lbs";

    private static String ZK_CONFIG_FILE_NAME = "/zk-create.yaml";
    private static CuratorFramework framework;
    //private static Map<String, Object> zkConfig;
    private static Map<String, Object> zkConfig = new HashMap<String, Object>();
    //initialized by shenhai.yan for line 40 NullPointerException

    public static void main(String[] args) {
        String url;
        if (args != null && args.length > 0) {
            ZK_CONFIG_FILE_NAME = args[0];
            url = args[1];
        } else {
            url = zkConfig.containsKey(CONFIG_URL_KEY) ?
                (String) zkConfig.get(CONFIG_URL_KEY) :
                "127.0.0.1:2181";
        }

        zkConfig = loadZkConfig();
        framework = createConnection(url);

        createConfig(MYCAT_HOST_KEY, false, MYCAT_HOST_KEY);
        createConfig(MYCAT_ZONE_KEY, false, MYCAT_ZONE_KEY);
        createConfig(MYCAT_NODES_KEY, false, MYCAT_NODES_KEY);
        createConfig(MYCAT_CLUSTER_KEY, true, MYCAT_CLUSTER_KEY);
        createConfig(MYCAT_MYSQLS_KEY, true, MYCAT_MYSQLS_KEY);
        createConfig(MYCAT_MYSQL_GROUP_KEY, true, MYCAT_MYSQL_GROUP_KEY);
        createConfig(MYCAT_LBS, true, MYCAT_LBS);
    }

    private static void createConfig(String configKey, boolean filterInnerMap,
        String configDirectory, String... restDirectory) {
        String childPath = ZKPaths.makePath("/", configDirectory, restDirectory);
        LOGGER.trace("child path is {}", childPath);

        try {
            ZKPaths.mkdirs(framework.getZookeeperClient().getZooKeeper(), childPath);

            Object mapObject = zkConfig.get(configKey);
            //recursion sub map
            if (mapObject instanceof Map) {
                createChildConfig(mapObject, filterInnerMap, childPath);
                return;
            }

            if (mapObject != null) {
                framework.setData()
                    .forPath(childPath, new JSONObject(mapObject).toString().getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createChildConfig(Object mapObject, boolean filterInnerMap,
        String childPath) {
        if (mapObject instanceof Map) {
            Map<Object, Object> innerMap = (Map<Object, Object>) mapObject;
            for (Map.Entry<Object, Object> entry : innerMap.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    createChildConfig(entry.getValue(), filterInnerMap,
                        ZKPaths.makePath(childPath, String.valueOf(entry.getKey())));
                } else {
                    LOGGER.trace("sub child path is {}", childPath);
                    processLeafNode(innerMap, filterInnerMap, childPath);
                }
            }
        }
    }

    private static void processLeafNode(Map<Object, Object> innerMap, boolean filterInnerMap,
        String childPath) {
        try {
            Stat restNodeStat = framework.checkExists().forPath(childPath);
            if (restNodeStat == null) {
                framework.create().creatingParentsIfNeeded().forPath(childPath);
            }

            if (filterInnerMap) {
                Map<Object, Object> filtered = new HashMap<>();
                for (Map.Entry<Object, Object> entry : innerMap.entrySet()) {
                    if (!(entry.getValue() instanceof Map)) {
                        filtered.put(entry.getKey(), entry.getValue());
                    }
                }

                framework.setData()
                    .forPath(childPath, new JSONObject(filtered).toString().getBytes());
            } else {
                framework.setData()
                    .forPath(childPath, new JSONObject(innerMap).toString().getBytes());
            }
        } catch (Exception e) {
            LOGGER.error("create node error: {} ", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked") private static Map<String, Object> loadZkConfig() {
        InputStream configIS = ZkCreate.class.getResourceAsStream(ZK_CONFIG_FILE_NAME);
        if (configIS == null) {
            throw new RuntimeException("can't find zk properties file : " + ZK_CONFIG_FILE_NAME);
        }
        return (Map<String, Object>) new Yaml().load(configIS);
    }

    private static CuratorFramework createConnection(String url) {
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
