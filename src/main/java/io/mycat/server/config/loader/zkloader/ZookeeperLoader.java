package io.mycat.server.config.loader.zkloader;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import io.mycat.locator.ZookeeperServiceLocator;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.loader.ConfigLoader;
import io.mycat.server.config.node.CharsetConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.config.node.DataNodeConfig;
import io.mycat.server.config.node.HostIndexConfig;
import io.mycat.server.config.node.QuarantineConfig;
import io.mycat.server.config.node.RuleConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SequenceConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.config.node.UserConfig;

public class ZookeeperLoader implements ConfigLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLoader.class);
    private static final String ZK_CONFIG_FILE_NAME = "/zk.yaml";

    private SystemConfig systemConfig;
    private Map<String, UserConfig> userConfigs;
    private Map<String, DataNodeConfig> dataNodeConfigs;
    private Map<String, DataHostConfig> dataHostConfigs;
    private Map<String, RuleConfig> ruleConfigs;
    private Map<String, SchemaConfig> schemaConfigs;

    public ZookeeperLoader() {
        super();
    }

    public void initConfig() {
        final ZkConfig zkConfig = loadZkConfig();
        final CuratorFramework zkConnection = ZookeeperServiceLocator.createConnection(zkConfig.getZkURL());
        final String myClusterId = zkConfig.getClusterID();

        //system config
        ZkSystemConfigLoader zkSystemLoader = new ZkSystemConfigLoader(myClusterId);
        //user config
        ZkUserConfigLoader zkUserConfigLoader = new ZkUserConfigLoader(myClusterId);

        //data host and data node config
        ZkDataNodeConfigLoader dataNodeConfigLoader = new ZkDataNodeConfigLoader(myClusterId
                , new ZkDataHostConfigLoader(myClusterId));

        //rule config
        ZkRuleConfigLoader ruleConfigLoader = new ZkRuleConfigLoader(myClusterId);

        // composed ZkDataHostConfigLoader,ZkDataNodeConfigLoader and ZkRuleConfigLoader
        ZkSchemaConfigLoader zkSchemaConfigLoader = new ZkSchemaConfigLoader(myClusterId,
                dataNodeConfigLoader, ruleConfigLoader);

        Arrays.asList(zkSystemLoader, zkUserConfigLoader, zkSchemaConfigLoader)
                .stream()
                .forEach(loader -> loader.fetchConfig(zkConnection));

        this.systemConfig = zkSystemLoader.getSystemConfig();
        this.userConfigs = zkUserConfigLoader.getUserConfig();
        this.dataHostConfigs = dataNodeConfigLoader.getDataHostConfigs();
        this.dataNodeConfigs = dataNodeConfigLoader.getDataNodeConfigs();
        this.schemaConfigs = zkSchemaConfigLoader.getSchemaConfigs();
    }

    private ZkConfig loadZkConfig() {
        LOGGER.trace("load file with name :" + ZK_CONFIG_FILE_NAME);

        InputStream configIS = getClass().getResourceAsStream(ZK_CONFIG_FILE_NAME);
        if (configIS == null) {
            throw new ConfigException("can't find zk properties file : " + ZK_CONFIG_FILE_NAME);
        }
        return new Yaml().loadAs(configIS, ZkConfig.class);
    }

    @Override
    public SchemaConfig getSchemaConfig(String schema) {
        return null;
    }

    @Override
    public Map<String, SchemaConfig> getSchemaConfigs() {
        return null;
    }

    @Override
    public Map<String, DataNodeConfig> getDataNodeConfigs() {
        return this.dataNodeConfigs;
    }

    @Override
    public Map<String, DataHostConfig> getDataHostConfigs() {
        return this.dataHostConfigs;
    }

    @Override
    public Map<String, RuleConfig> getTableRuleConfigs() {
        return this.ruleConfigs;
    }

    @Override
    public SystemConfig getSystemConfig() {
        return this.systemConfig;
    }

    @Override
    public UserConfig getUserConfig(String user) {
        return this.userConfigs.get(user);
    }

    @Override
    public Map<String, UserConfig> getUserConfigs() {
        return this.userConfigs;
    }

    @Override
    public QuarantineConfig getQuarantineConfigs() {
        return null;
    }

    @Override
    public MycatClusterConfig getClusterConfigs() {
        return null;
    }

    @Override
    public CharsetConfig getCharsetConfigs() {
        return null;
    }

    @Override
    public HostIndexConfig getHostIndexConfig() {
        return null;
    }

    @Override
    public SequenceConfig getSequenceConfig() {
        return null;
    }
}
