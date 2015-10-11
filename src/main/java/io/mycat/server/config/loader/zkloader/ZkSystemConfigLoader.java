package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;
import io.mycat.server.config.loader.SystemLoader;
import io.mycat.server.config.node.SystemConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * load system configuration from Zookeeper.
 * </p>
 * Created by v1.lion on 2015/9/27.
 */
public class ZkSystemConfigLoader extends AbstractZKLoaders implements SystemLoader {
    //directory name of server config in zookeeper
    protected static final String SERVER_CONFIG_DIRECTORY = "server-config";
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkSystemConfigLoader.class);
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
        this.systemConfig = JSON.parseObject(super.fetchData(zkConnection, SYSTEM_DIRECTORY)
                , SystemConfig.class);

        LOGGER.trace("done system config from zookeeper : {}", systemConfig);
    }

    public SystemConfig getSystemConfig() {
        return this.systemConfig;
    }
}
