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
public class ZkDatanodeConfigLoader extends AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkDatanodeConfigLoader.class);
    private Map<String, DataNodeConfig> dataNodeConfigs;

    public ZkDatanodeConfigLoader(final String clusterID) {
        super(clusterID, DATANODE_CONFIG_DIRECTORY);
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {
        //data node config path in zookeeperuser
        //example: / mycat-cluster-1/ datanode-config
        try {
            byte[] rawDataNode = zkConnection
                    .getData()
                    .forPath(BASE_CONFIG_PATH);
            String DataNodeObjStr = new String(rawDataNode, StandardCharsets.UTF_8);

            this.dataNodeConfigs = JSON.parseArray(DataNodeObjStr, DataNodeConfig.class)
                    .stream()
                    .collect(Collectors.toMap(DataNodeConfig::getName, Function.identity()));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new ConfigException(e);
        }
    }

    public Map<String, DataNodeConfig> getDataNodeConfigs() {
        return this.dataNodeConfigs;
    }
}
