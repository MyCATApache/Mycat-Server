package io.mycat.server.config.loader.zkloader;

import io.mycat.server.config.loader.SystemLoader;
import io.mycat.server.config.node.SystemConfig;

import javax.annotation.Nonnull;

/**
 * <p>
 * load system configuration from Zookeeper.
 * </p>
 * Created by v1.lion on 2015/9/27.
 */
public class ZkSystemLoader implements SystemLoader {

    private final String myID;
    private SystemConfig systemConfig;

    public ZkSystemLoader(@Nonnull String myID) {
        this.myID = myID;
    }

    /**
     * fetch config form zookeeper transform them to SystemConfig.
     *
     * @return SystemConfig
     */
    public SystemConfig getSystemConfig() {
        SystemConfig defaultConfig = new SystemConfig();


        return null;
    }



}
