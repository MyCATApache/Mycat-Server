package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.node.DataNodeConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by v1.lion on 2015/10/8.
 */
public class ZkDataNodeConfigLoader extends AbstractZKLoaders {
    //directory name of data node config in zookeeper
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkDataNodeConfigLoader.class);
    private static final String DATANODE_CONFIG_DIRECTORY = "datanode-config";

    //hold dataNode name mapping to DataNodeConfig
    private Map<String, DataNodeConfig> dataNodeConfigs;

    public ZkDataNodeConfigLoader(final String clusterID) {
        super(clusterID, DATANODE_CONFIG_DIRECTORY);
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {
        //data node config path in zookeeper
        //example: /mycat-cluster-1/datanode-config
        byte[] rawDataNode;
        try {
            rawDataNode = zkConnection
                    .getData()
                    .forPath(BASE_CONFIG_PATH);
        } catch (Exception e) {
            LOGGER.error("fetch data node config from zookeeper error : {}, path : {}",
                    e.getMessage(), BASE_CONFIG_PATH);
            throw new ConfigException(e);
        }

        this.dataNodeConfigs = JSON.parseArray(new String(rawDataNode, StandardCharsets.UTF_8)
                , DataNodeConfig.class)
                .stream()
                .collect(Collectors.toMap(DataNodeConfig::getName, Function.identity()));
    }

    public Map<String, DataNodeConfig> getDataNodeConfigs() {
        return this.dataNodeConfigs;
    }
}
