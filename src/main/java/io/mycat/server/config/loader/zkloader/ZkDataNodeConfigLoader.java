package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.mycat.server.config.ConfigException;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.config.node.DataNodeConfig;

import static java.util.stream.Collectors.toMap;

/**
 * Created by v1.lion on 2015/10/8.
 */
public class ZkDataNodeConfigLoader extends AbstractZKLoaders {
    //directory name of data node config in zookeeper
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkDataNodeConfigLoader.class);
    private static final String DATANODE_CONFIG_DIRECTORY = "datanode-config";
    private final ZkDataHostConfigLoader dataHostConfigLoader;

    //hold dataNode name mapping to DataNodeConfig
    private Map<String, DataNodeConfig> dataNodeConfigs;

    public ZkDataNodeConfigLoader(final String clusterID, ZkDataHostConfigLoader dataHostConfigLoader) {
        super(clusterID, DATANODE_CONFIG_DIRECTORY);
        this.dataHostConfigLoader = dataHostConfigLoader;
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {
        //invoke composed
        this.dataHostConfigLoader.fetchConfig(zkConnection);

        //data node config path in zookeeper
        //example: /mycat-cluster-1/datanode-config
        this.dataNodeConfigs = new HashMap<>();

        JSON.parseArray(super.fetchDataToString(zkConnection, "")
                , DataNodeConfig.class)
                .stream()
                .forEach(dataNodeConfig -> {
                    if (dataNodeConfigs.containsKey(dataNodeConfig.getName())) {
                        throw new ConfigException("dataNode " + dataNodeConfig.getName() +
                                " duplicated!");
                    }

                    if (!dataHostConfigLoader.getDataHostConfigs()
                            .containsKey(dataNodeConfig.getDataHost())) {
                        throw new ConfigException("dataNode " + dataNodeConfig.getName() +
                                " reference dataHost:" + dataNodeConfig.getDataHost() +
                                " not exists!");
                    }

                    dataNodeConfigs.put(dataNodeConfig.getName(),dataNodeConfig);
                });
        LOGGER.trace("done fetch data node config.");
    }

    public Map<String, DataNodeConfig> getDataNodeConfigs() {
        return this.dataNodeConfigs;
    }

    public Map<String, DataHostConfig> getDataHostConfigs() {
        return this.dataHostConfigLoader.getDataHostConfigs();
    }

}
