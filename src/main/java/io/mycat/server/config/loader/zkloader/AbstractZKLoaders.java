package io.mycat.server.config.loader.zkloader;

import io.mycat.server.config.ConfigException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

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
     * return a string transformed from data in specified path.
     *
     * @param zkConnection
     * @param path         path in zookeeper
     * @return data string
     */
    public String fetchDataToString(final CuratorFramework zkConnection, String path, String... restPath) {
        return new String(fetchData(zkConnection, path, restPath), StandardCharsets.UTF_8);
    }

    /**
     * return data based on specified path.
     *
     * @param zkConnection
     * @param path
     * @param restPath
     * @return data in zookeeper
     */
    public byte[] fetchData(final CuratorFramework zkConnection, String path, String... restPath) {
        String dataPath = ZKPaths.makePath(BASE_CONFIG_PATH, path, restPath);
        try {
            byte[] rawByte = zkConnection.getData().forPath(dataPath);
            LOGGER.trace("get raw data from zookeeper: {} , path : {}",
                    new String(rawByte, StandardCharsets.UTF_8), dataPath);
            return rawByte;
        } catch (Exception e) {
            LOGGER.error("get config data from zookeeper error : {}, path : {}",
                    e.getMessage(), dataPath);
            throw new ConfigException(e);
        }
    }


    /**
     * return a children name list under BASE_CONFIG_PATH
     *
     * @param zkConnection
     * @param restPath     rest path concat to BASE_CONFIG_PATH
     * @return name list
     */
    public List<String> fetchChildren(final CuratorFramework zkConnection, String... restPath) {
        try {
            String childPath = ZKPaths.makePath(BASE_CONFIG_PATH, null, restPath);
            return zkConnection
                    .getChildren()
                    .forPath(childPath);
        } catch (Exception e) {
            LOGGER.error("fetch child node name from zookeeper error : {} , path {} ", e.getMessage(), BASE_CONFIG_PATH);
            throw new ConfigException(e);
        }
    }

    /**
     * fetch config form zookeeper and then transform them to bean.
     *
     * @param zkConnection a zookeeper connection
     */
    abstract public void fetchConfig(final CuratorFramework zkConnection);
}
