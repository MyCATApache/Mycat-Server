package io.mycat.server.config.loader.zkloader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * provide a abstract constructor to construct zookeeper path.
 * Created by v1.lion on 2015/10/8.
 */
public abstract class AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractZKLoaders.class);

    //parent config path in zookeeper
    //example /mycat-cluster-/server-config/
    // /CLUSTER_ID/CONFIG_DIRECTORY_NAME
    protected final String BASE_CONFIG_PATH;

    public AbstractZKLoaders() {
        super();
        BASE_CONFIG_PATH = null;
    }

    public AbstractZKLoaders(String clusterID, String configDirectoryName) {
        BASE_CONFIG_PATH = ZKPaths.makePath("/", clusterID, configDirectoryName);
        LOGGER.trace("base config path is {}", BASE_CONFIG_PATH);
    }

    /**
     * fetch config form zookeeper and then transform them to bean.
     *
     * @param zkConnection a zookeeper connection
     */
    abstract public void fetchConfig(final CuratorFramework zkConnection);
}
