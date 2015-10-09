package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.loader.SystemLoader;
import io.mycat.server.config.node.SystemConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * load system configuration from Zookeeper.
 * </p>
 * Created by v1.lion on 2015/9/27.
 */
public class ZkSystemConfigLoader extends AbstractZKLoaders implements SystemLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkSystemConfigLoader.class);

    //directory name of server config in zookeeper
    protected static final String SERVER_CONFIG_DIRECTORY = "server-config";
    //directory name of system config in zookeeper
    private static final String SYSTEM_DIRECTORY = "system";

    private SystemConfig systemConfig;

    public ZkSystemConfigLoader(final String clusterID) {
        super(clusterID, SERVER_CONFIG_DIRECTORY);
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {

        //system config path in zookeeper
        //example: /mycat-cluster-1 /server-config/system
        String systemConfigPath = ZKPaths.makePath(BASE_CONFIG_PATH, SYSTEM_DIRECTORY);

        LOGGER.trace("fetch system config from zookeeper with path: {}", systemConfigPath);
        try {
            byte[] systemValue = zkConnection.getData().forPath(systemConfigPath);
            this.systemConfig = JSON.parseObject(systemValue, SystemConfig.class);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new ConfigException(e);
        }
    }

    public SystemConfig getSystemConfig() {
        return this.systemConfig;
    }
}
