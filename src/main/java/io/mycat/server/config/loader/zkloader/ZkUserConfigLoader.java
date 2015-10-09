package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.node.UserConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>
 * load user configuration from Zookeeper.
 * </p>
 */
public class ZkUserConfigLoader extends AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkUserConfigLoader.class);
    //directory name of user config in zookeeper
    private static final String USERS_DIRECTORY = "user";

    private Map<String, UserConfig> userConfigs;

    public ZkUserConfigLoader(final String clusterID) {
        super(clusterID, SERVER_CONFIG_DIRECTORY);
    }


    @Override
    public void fetchConfig(CuratorFramework zkConnection) {

        //user config path in zookeeper
        //example: /mycat-cluster-1 /server-config/user
        String usersPath = ZKPaths.makePath(BASE_CONFIG_PATH, USERS_DIRECTORY);

        try {
            this.userConfigs = zkConnection
                    .getChildren()
                    .forPath(usersPath)
                    .stream()
                    .map(username -> ZKPaths.makePath(usersPath, username))
                    .map(userPath -> {
                        try {
                            LOGGER.trace("fetch user config from zookeeper with path: {}", userPath);
                            byte[] userRawData = zkConnection.getData().forPath(userPath);
                            return (UserConfig) JSON.parseObject(userRawData, UserConfig.class);
                        } catch (Exception e) {
                            LOGGER.error("fetch user config error!", e);
                            throw new ConfigException(e);
                        }
                    })
                    .collect(
                            Collectors.toMap(UserConfig::getName, Function.identity())
                    );
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new ConfigException(e);
        }
    }

    public Map<String, UserConfig> getUserConfig() {
        return userConfigs;
    }
}
