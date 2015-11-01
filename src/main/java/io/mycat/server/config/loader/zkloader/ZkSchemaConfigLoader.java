package io.mycat.server.config.loader.zkloader;

import com.google.common.collect.ObjectArrays;

import com.alibaba.fastjson.JSON;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.TableConfig;

/**
 *
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
                .map(this::createSchema)
                .collect(Collectors.toMap(SchemaConfig::getName, Function.identity()));
    }

    private SchemaConfig createSchema(final String schemaName) {
        //parse schema
        //mycat-cluster-1/ schema-config/ ${schema name}
        SchemaConfig schemaConfig = JSON.parseObject(
                super.fetchData(this.zkConnection, schemaName), SchemaConfig.class);

        //parse TableConfig
        //mycat-cluster-1/ schema-config/ ${schema name} /${table name}
        Map<String, TableConfig> tables = super.fetchChildren(this.zkConnection, schemaName)
                .stream()
                .flatMap(tableName -> generateTable(schemaName, tableName))
                .collect(Collectors.toMap(TableConfig::getName, Function.identity()));

        schemaConfig.setTables(tables);
        return schemaConfig;
    }

    /**
     * create parent tableConfig.
     */
    private Stream<TableConfig> generateTable(final String schemaName, final String tableName) {
        TableConfig parentTableConfig = createTableConfig(schemaName, tableName);

        return Stream.concat(
                super.fetchChildren(zkConnection, schemaName, tableName)
                        .stream()
                        .flatMap(childTableName -> generateChildTable(parentTableConfig,
                                schemaName, tableName, childTableName)),
                Stream.of(parentTableConfig));
    }

    /**
     * create child tableConfig.
     */
    private Stream<TableConfig> generateChildTable(TableConfig parentTableConfig,
                                                   final String schemaName,
                                                   final String... childTableName) {
        //recursion parse child TableConfig
        //mycat-cluster-1/ schema-config/ ${schema name} /${table name} /${child table name}
        //deep first.
        TableConfig childTableConfig = createTableConfig(schemaName, childTableName);
        childTableConfig.setParentTC(parentTableConfig);

        return Stream.concat(
                super.fetchChildren(zkConnection,
                        ObjectArrays.concat(new String[]{schemaName}, childTableName, String.class))
                        .stream()
                        .flatMap(
                                grandChildTableName -> generateChildTable(childTableConfig,
                                        schemaName,
                                        ObjectArrays.concat(childTableName, new String[]{grandChildTableName}, String.class))
                        )
                , Stream.of(childTableConfig));
    }

    private TableConfig createTableConfig(String schemaName, String... tableName) {
        TableConfig tableConfig = JSON.parseObject(
                super.fetchData(this.zkConnection, schemaName, tableName), TableConfig.class);
        tableConfig.setRule(this.ruleConfigLoadr.getRuleConfigs().get(tableConfig.getRuleName()));
        tableConfig.checkConfig();
        return tableConfig;
    }

    public Map<String, SchemaConfig> getSchemaConfigs() {
        return schemaConfigs;
    }
}
