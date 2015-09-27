package io.mycat.server.config.loader.zkloader;

import com.google.common.base.Strings;
import com.google.common.io.CharSource;
import com.google.common.io.Resources;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.loader.ConfigLoader;
import io.mycat.server.config.loader.SystemLoader;
import io.mycat.server.config.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ZookeeperLoader implements ConfigLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLoader.class);
    private static final String ZK_ID_FILENAME = "myid";

    private SystemConfig systemConfig;

    public ZookeeperLoader() {
        String myId;
        try {
            //read myid file.
            URL fileUrl = Resources.getResource(ZK_ID_FILENAME);
            CharSource fileContents = Resources.asCharSource(fileUrl, StandardCharsets.UTF_8);
            myId = fileContents.read();
        } catch (IOException e) {
            LOGGER.error("occur a I/O error during reading file!", e);
            throw new ConfigException(e);
        }

        if (Strings.isNullOrEmpty(myId)) {
            LOGGER.error("myid file is null or empty!");
            throw new ConfigException("myid file is null or empty!");
        }

        SystemLoader zkSystemLoader = new ZkSystemLoader(myId);
        this.systemConfig = zkSystemLoader.getSystemConfig();
    }

    @Override
    public SchemaConfig getSchemaConfig(String schema) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, SchemaConfig> getSchemaConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, DataNodeConfig> getDataNodeConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, DataHostConfig> getDataHostConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, RuleConfig> getTableRuleConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SystemConfig getSystemConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UserConfig getUserConfig(String user) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, UserConfig> getUserConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QuarantineConfig getQuarantineConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MycatClusterConfig getClusterConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CharsetConfig getCharsetConfigs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HostIndexConfig getHostIndexConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SequenceConfig getSequenceConfig() {
        // TODO Auto-generated method stub
        return null;
    }

}
