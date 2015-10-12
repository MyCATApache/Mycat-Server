package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;
import io.mycat.server.config.node.DataNodeConfig;
import io.mycat.server.config.node.SchemaConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Created by v1.lion on 2015/10/8.
 */
public class ZkSchemaConfigLoader extends AbstractZKLoaders {
    //directory name of data node config in zookeeper
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkSchemaConfigLoader.class);
    private static final String SCHEMA_CONFIG_DIRECTORY = "schema-config";

    //hold schema name mapping to DataNodeConfig
    private Map<String, SchemaConfig> schemaConfigs;

    public ZkSchemaConfigLoader(final String clusterID) {
        super(clusterID, SCHEMA_CONFIG_DIRECTORY);
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {
        //data node config path in zookeeper
        //example: /mycat-cluster-1/schema-config
        super.fetchChildren(zkConnection)
                .stream();
    }

    public Map<String, SchemaConfig> getSchemaConfigs() {
        return schemaConfigs;
    }
}
