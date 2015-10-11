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

import static java.util.stream.Collectors.toMap;

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

        this.userConfigs = super
                .fetchChildren(zkConnection, USERS_DIRECTORY)
                .stream()
                .map(username -> (UserConfig) JSON.parseObject(
                        super.fetchData(zkConnection, USERS_DIRECTORY, username), UserConfig.class))
                .collect(toMap(UserConfig::getName, Function.identity()));

        LOGGER.trace("done fetch user config : {}", this.userConfigs);
    }

    public Map<String, UserConfig> getUserConfig() {
        return userConfigs;
    }
}
