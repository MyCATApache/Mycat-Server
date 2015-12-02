package demo.catlets;

import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
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

    private static final String MYCATZONE_CONFIG_DIRECTORY = "mycat-zones-config";
    private static final String MYCATHOST_CONFIG_DIRECTORY = "mycat-hosts-config";
    private static final String MYCATNODE_CONFIG_DIRECTORY = "mycat-nodes-config";
    private static final String MYCATLB_CONFIG_DIRECTORY = "mycatlbs-config";
    private static final String MYCATMYSQLS_CONFIG_DIRECTORY = "mycat-mysqls-config";
    private static final String MYCATMYSQL_GROUP_CONFIG_DIRECTORY = "mycat-mysqlgroup-config";
    private static final String SERVER_CONFIG_DIRECTORY = "server-config";
    private static final String DATANODE_CONFIG_DIRECTORY = "datanode-config";
    private static final String RULE_CONFIG_DIRECTORY = "rule-config";
    private static final String SEQUENCE_CONFIG_DIRECTORY = "sequence-config";
    private static final String SCHEMA_CONFIG_DIRECTORY = "schema-config";
    private static final String DATAHOST_CONFIG_DIRECTORY = "datahost-config";
    private static final String MYSQLREP_CONFIG_DIRECTORY = "mysqlrep-config";


    private static final String CONFIG_MYCAT_KEY = "zkZone";
    private static final String CONFIG_MYCATZONE_KEY = "mycat-zones";
    private static final String CONFIG_MYCATHOST_KEY = "mycat-hosts";
    private static final String CONFIG_MYCATNODE_KEY = "mycat-nodes";
    private static final String CONFIG_MYCATMYSQLS_KEY = "mycat-mysqls";
    private static final String CONFIG_MYCATMYSQL_GROUP_KEY = "mycat-mysqlgroup";
    private static final String CONFIG_MYSQLREP_KEY = "mysqlrep";
    private static final String CONFIG_MYCATLB_KEY = "mycat-lbs";
    private static final String CONFIG_URL_KEY = "zkUrl";
    private static final String CONFIG_CLUSTER_KEY = "zkClu";
    private static final String CONFIG_CLUSTER_ID = "zkID";
    private static final String CONFIG_ZK_USERD = "zkUsed";
    private static final String CONFIG_SYSTEM_KEY = "system";
    private static final String CONFIG_USER_KEY = "user";
    private static final String CONFIG_DATANODE_KEY = "datanode";
    private static final String CONFIG_RULE_KEY = "rule";
    private static final String CONFIG_SEQUENCE_KEY = "sequence";
    private static final String CONFIG_SCHEMA_KEY = "schema";
    private static final String CONFIG_DATAHOST_KEY = "datahost";



    private static String CLU_PARENT_PATH;
    private static String ZONE_PARENT_PATH;
    private static String SERVER_PARENT_PATH;

    private static CuratorFramework framework;
    private static Map<String, Object> zkConfig;
    
    public static void main(String[] args) {
//
//    	boolean zkcreate = ZkCreate.init();
//        if (!zkcreate)
//            System.exit(1);
        LOGGER.info("start zkcreate ");
        zkConfig = loadZkConfig();
        LOGGER.info("need to zkcreate  to remote center ");
        ZONE_PARENT_PATH = ZKPaths.makePath("/", String.valueOf(zkConfig.get(CONFIG_MYCAT_KEY)));

        CLU_PARENT_PATH = ZKPaths.makePath(ZONE_PARENT_PATH + "/", String.valueOf(zkConfig.get(CONFIG_CLUSTER_KEY)));
        LOGGER.info("parent path is {}", CLU_PARENT_PATH);
        framework = createConnection((String) zkConfig.get(CONFIG_URL_KEY));

        createConfig(ZONE_PARENT_PATH,CONFIG_MYCATZONE_KEY, true, MYCATZONE_CONFIG_DIRECTORY);
        createConfig(ZONE_PARENT_PATH,CONFIG_MYCATHOST_KEY, true, MYCATHOST_CONFIG_DIRECTORY);
        createConfig(ZONE_PARENT_PATH,CONFIG_MYCATNODE_KEY, true, MYCATNODE_CONFIG_DIRECTORY);
        createConfig(ZONE_PARENT_PATH,CONFIG_MYCATLB_KEY, true, MYCATLB_CONFIG_DIRECTORY);
        createConfig(ZONE_PARENT_PATH,CONFIG_MYCATMYSQLS_KEY, true, MYCATMYSQLS_CONFIG_DIRECTORY);
        createConfig(ZONE_PARENT_PATH,CONFIG_MYCATMYSQL_GROUP_KEY, true, MYCATMYSQL_GROUP_CONFIG_DIRECTORY);

    }

    //process zkcreate
    public static boolean init( ){
        LOGGER.info("start zkcreate ");
        zkConfig = loadZkConfig();
        boolean isUsed = (boolean) zkConfig.get(CONFIG_ZK_USERD);
        if(isUsed){
        	if(ZkDownload.init()==false){
        		LOGGER.info("need to zkcreate  to remote center ");
            	ZONE_PARENT_PATH = ZKPaths.makePath("/", String.valueOf(zkConfig.get(CONFIG_MYCAT_KEY)));

                CLU_PARENT_PATH = ZKPaths.makePath(ZONE_PARENT_PATH + "/", String.valueOf(zkConfig.get(CONFIG_CLUSTER_KEY)));
                LOGGER.info("parent path is {}", CLU_PARENT_PATH);
                framework = createConnection((String) zkConfig.get(CONFIG_URL_KEY));

                createConfig(CLU_PARENT_PATH,CONFIG_DATANODE_KEY, true, DATANODE_CONFIG_DIRECTORY);
                createConfig(CLU_PARENT_PATH,CONFIG_SYSTEM_KEY, true, SERVER_CONFIG_DIRECTORY, CONFIG_SYSTEM_KEY);
                createConfig(CLU_PARENT_PATH,CONFIG_USER_KEY, true, SERVER_CONFIG_DIRECTORY, CONFIG_USER_KEY);
                createConfig(CLU_PARENT_PATH,CONFIG_SEQUENCE_KEY, true, SEQUENCE_CONFIG_DIRECTORY);
                createConfig(CLU_PARENT_PATH,CONFIG_SCHEMA_KEY, true, SCHEMA_CONFIG_DIRECTORY);
                createConfig(CLU_PARENT_PATH,CONFIG_DATAHOST_KEY, true, DATAHOST_CONFIG_DIRECTORY);
                createConfig(CLU_PARENT_PATH,CONFIG_RULE_KEY, false, RULE_CONFIG_DIRECTORY);

                createConfig(ZONE_PARENT_PATH,CONFIG_MYSQLREP_KEY, false, MYSQLREP_CONFIG_DIRECTORY);
                LOGGER.info("parent path is {}", ZONE_PARENT_PATH);

                createConfig(ZONE_PARENT_PATH,CONFIG_MYCATLB_KEY, false, MYCATLB_CONFIG_DIRECTORY);
                LOGGER.info("zkcreate  to remote center end ...");
        	}
        }else{
        	LOGGER.info("dont't need to zkcreate  to remote center ");
        }
        
        return true;
    }

    private static void createConfig(String parent_path,String configKey, boolean filterInnerMap, String... configDirectory) {
        String childPath = ZKPaths.makePath(parent_path, null, configDirectory);
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

    @SuppressWarnings("unchecked")
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
