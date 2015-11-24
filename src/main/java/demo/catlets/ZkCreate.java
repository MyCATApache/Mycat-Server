package demo.catlets;

import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.opencloudb.config.model.ZkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by v1.lion on 2015/10/7.
 */
public class ZkCreate {
    private static final String ZK_CONFIG_FILE_NAME = "/zk-create.yaml";
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkCreate.class);

    private static final String SERVER_CONFIG_DIRECTORY = "server-config";
    private static final String DATANODE_CONFIG_DIRECTORY = "datanode-config";
    private static final String RULE_CONFIG_DIRECTORY = "rule-config";
    private static final String SEQUENCE_CONFIG_DIRECTORY = "sequence-config";
    private static final String SCHEMA_CONFIG_DIRECTORY = "schema-config";
    private static final String DATAHOST_CONFIG_DIRECTORY = "datahost-config";

    private static final String CONFIG_URL_KEY = "zkUrl";
    private static final String CONFIG_CLUSTER_KEY = "zkClu";
    private static final String CONFIG_CLUSTER_ID = "zkID";
    private static final String CONFIG_SYSTEM_KEY = "system";
    private static final String CONFIG_USER_KEY = "user";
    private static final String CONFIG_DATANODE_KEY = "datanode";
    private static final String CONFIG_RULE_KEY = "rule";
    private static final String CONFIG_SEQUENCE_KEY = "sequence";
    private static final String CONFIG_SCHEMA_KEY = "schema";
    private static final String CONFIG_DATAHOST_KEY = "datahost";

    private static String PARENT_PATH;

    private static CuratorFramework framework;
    private static Map<String, Object> zkConfig;

    public static void main(String[] args) {

        ZkConfig zkConfig1 = new ZkConfig();
        zkConfig1.isUsedZk();
        if (zkConfig1.getZkUrl() != null && zkConfig1.getZkID()!=null&& zkConfig1.getZkPort()!=null&& zkConfig1.getZkClu()!=null) {
            //upload data to zk,only use in init data...
            boolean zkcreate = ZkCreate.init(zkConfig1.getZkUrl(), zkConfig1.getZkPort(), zkConfig1.getZkClu(), zkConfig1.getZkID());
            if (!zkcreate)
                System.exit(1);
        }

    }

    //process zkcreate
    public static boolean init(String zkUrl,String zkPort,String zkClu,String zkID){
        LOGGER.info("-----start zkcreate-----");
        zkConfig = loadZkConfig();
        if (!zkConfig.get("zkUrl").toString().equals(zkUrl+":"+zkPort)||!zkConfig.get("zkClu").toString().equals(zkClu)||!zkConfig.get("zkID").toString().equals(zkID)){
            LOGGER.trace("zk config is not match, please check it!!");
            return false;
        }else {
            PARENT_PATH = ZKPaths.makePath("/", String.valueOf(zkConfig.get(CONFIG_CLUSTER_KEY)+"/"+String.valueOf(zkConfig.get(CONFIG_CLUSTER_ID))));
            LOGGER.info("parent path is {}", PARENT_PATH);
            framework = createConnection((String) zkConfig.get(CONFIG_URL_KEY));
            createConfig(CONFIG_DATANODE_KEY, true, DATANODE_CONFIG_DIRECTORY);
            createConfig(CONFIG_SYSTEM_KEY, true, SERVER_CONFIG_DIRECTORY, CONFIG_SYSTEM_KEY);
            createConfig(CONFIG_USER_KEY, true, SERVER_CONFIG_DIRECTORY, CONFIG_USER_KEY);
            createConfig(CONFIG_SEQUENCE_KEY, true, SEQUENCE_CONFIG_DIRECTORY);
            createConfig(CONFIG_SCHEMA_KEY, true, SCHEMA_CONFIG_DIRECTORY);
            createConfig(CONFIG_DATAHOST_KEY, true, DATAHOST_CONFIG_DIRECTORY);
            createConfig(CONFIG_RULE_KEY, false, RULE_CONFIG_DIRECTORY);
            return true;
        }
    }

    private static void createConfig(String configKey, boolean filterInnerMap, String... configDirectory) {
        String childPath = ZKPaths.makePath(PARENT_PATH, null, configDirectory);
        LOGGER.trace("child path is {}", childPath);

        try {
            Stat systemPropertiesNodeStat = framework.checkExists().forPath(childPath);
            if (systemPropertiesNodeStat == null) {
                framework.create().creatingParentsIfNeeded().forPath(childPath);
            }

            Object mapObject = zkConfig.get(configKey);
            //recursion sub map
            if (mapObject instanceof Map) {
                createChildConfig(mapObject, filterInnerMap, childPath);
                return;
            }

            framework.setData().forPath(childPath, JSON.toJSONString(mapObject).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createChildConfig(Object mapObject, boolean filterInnerMap, String childPath) {
        if (mapObject instanceof Map) {
            Map<Object, Object> innerMap = (Map<Object, Object>) mapObject;
            Iterator<Map.Entry<Object, Object>> it = innerMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Object, Object> entry = it.next();
                if (entry.getValue() instanceof Map) {
                    createChildConfig(entry.getValue(), filterInnerMap, ZKPaths.makePath(childPath, String.valueOf(entry.getKey())));
                } else {
                    LOGGER.trace("sub child path is {}", childPath);
                    processLeafNode(innerMap, filterInnerMap, childPath);

                }
            }
        }
    }

    private static void processLeafNode(Map<Object, Object> innerMap, boolean filterInnerMap, String childPath) {
        try {
            Stat restNodeStat = framework.checkExists().forPath(childPath);
            if (restNodeStat == null) {
                framework.create().creatingParentsIfNeeded().forPath(childPath);
            }
//
//            if (filterInnerMap) {
                Iterator<Map.Entry<Object, Object>> it = innerMap.entrySet().iterator();
                framework.setData().forPath(childPath, JSON.toJSONString(innerMap).getBytes());
//            } else {
//                framework.setData().forPath(childPath, JSON.toJSONString(innerMap).getBytes());
//            }
        } catch (Exception e) {
            LOGGER.error("create node error: {} ", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> loadZkConfig() {
        InputStream configIS = ZkCreate.class.getResourceAsStream(ZK_CONFIG_FILE_NAME);
        if (configIS == null) {
            throw new RuntimeException("can't find zk properties file : " + ZK_CONFIG_FILE_NAME);
        }
        return (Map<String, Object>) new Yaml().load(configIS);
    }

    private static CuratorFramework createConnection(String url) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .newClient(url, new ExponentialBackoffRetry(100, 6));

        //start connection
        curatorFramework.start();
        //wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework;
            }
        } catch (InterruptedException e) {
        }

        //fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }
}
