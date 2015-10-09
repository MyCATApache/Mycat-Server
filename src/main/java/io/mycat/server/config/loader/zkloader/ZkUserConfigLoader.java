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
 * Created by v1.lion on 2015/9/27.
 */
public class ZkUserConfigLoader extends AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkUserConfigLoader.class);

    //directory name of user config in zookeeper
    private static final String USERS_DIRECTORY = "user";

    //hold user name mapping to UserConfig
    private Map<String, UserConfig> userConfigs;

    public ZkUserConfigLoader(final String clusterID) {
        super(clusterID, ZkSystemConfigLoader.SERVER_CONFIG_DIRECTORY);
    }


    @Override
    public void fetchConfig(CuratorFramework zkConnection) {

        //user config path in zookeeper
        //example: /mycat-cluster-1/server-config/user
        String usersPath = ZKPaths.makePath(BASE_CONFIG_PATH, USERS_DIRECTORY);


        try {
            this.userConfigs = zkConnection
                    .getChildren()
                    .forPath(usersPath)
                    .stream()
                    .map(username -> ZKPaths.makePath(usersPath, username))
                    .map(userPath -> {
                        byte[] userRawData;
                        try {
                            userRawData = zkConnection.getData().forPath(userPath);
                            LOGGER.trace("fetch user config from zookeeper with path: {} and raw data : {}"
                                    , userPath, String.valueOf(userRawData));
                        } catch (Exception e) {
                            LOGGER.error("fetch user config from zookeeper error: {}", e.getMessage(), e);
                            throw new ConfigException(e);
                        }

                        return (UserConfig) JSON.parseObject(userRawData, UserConfig.class);
                    })
                    .collect(
                            Collectors.toMap(UserConfig::getName, Function.identity())
                    );
        } catch (Exception e) {
            LOGGER.error("fetch user child node from zookeeper error : {} , path {} ", e.getMessage(), usersPath);
            throw new ConfigException(e);
        }

    }

    public Map<String, UserConfig> getUserConfig() {
        return userConfigs;
    }
}
