package org.opencloudb.config;

import org.opencloudb.config.loader.zookeeper.ZookeeperLoader;

/**
 * Created by StoneGod on 2015/11/23.
 */
public class ZkConfig {
    private ZkConfig() {
    }

    public synchronized static ZkConfig instance() {
        return new ZkConfig();
    }

    public void initZk() {
        try {
            new ZookeeperLoader().buildConfig();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
