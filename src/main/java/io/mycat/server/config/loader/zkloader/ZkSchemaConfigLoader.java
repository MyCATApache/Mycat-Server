package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.mycat.server.config.node.SchemaConfig;

/**
 * Created by v1.lion on 2015/10/18.
 */
public class ZkSchemaConfigLoader extends AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkSchemaConfigLoader.class);

    //directory name of data node config in zookeeper
    private static final String SCHEMA_CONFIG_DIRECTORY = "schema-config";

    ZkDataNodeConfigLoader dataNodeConfigLoader;
    ZkRuleConfigLoader ruleConfigLoadr;

    //hold a zookeeper connection,it is be closed after initiation
    CuratorFramework zkConnection;

    //hold schema name mapping to DataNodeConfig
    private Map<String, SchemaConfig> schemaConfigs;

    public ZkSchemaConfigLoader(final String clusterID,
                                ZkDataNodeConfigLoader dataNodeConfigLoader,
                                ZkRuleConfigLoader ruleConfigLoader) {
        super(clusterID, SCHEMA_CONFIG_DIRECTORY);
        this.ruleConfigLoadr = ruleConfigLoader;
        this.dataNodeConfigLoader = dataNodeConfigLoader;
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {
        this.ruleConfigLoadr.fetchConfig(zkConnection);
        this.dataNodeConfigLoader.fetchConfig(zkConnection);
        this.zkConnection = zkConnection;

        //data node config path in zookeeper
        //example: /mycat-cluster-1/schema-config / ${schemaName}
        this.schemaConfigs = super.fetchChildren(zkConnection)
                .stream()
                .map(schemaName -> createSchema(schemaName))
                .collect(Collectors.toMap(SchemaConfig::getName, Function.identity()));
    }

    private SchemaConfig createSchema(final String schemaName) {
        //parse schema
        SchemaConfig schemaConfig = JSON.parseObject(
                super.fetchData(this.zkConnection, schemaName), SchemaConfig.class);

        return null;
    }

    public Map<String, SchemaConfig> getSchemaConfigs() {
        return schemaConfigs;
    }
}
