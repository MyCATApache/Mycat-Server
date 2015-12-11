package org.opencloudb.config;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.opencloudb.config.loader.zookeeper.ZookeeperLoader;
import org.opencloudb.config.loader.zookeeper.ZookeeperSaver;

public class ZkConfig {
    private static final Logger LOGGER = Logger.getLogger(ZkConfig.class);

    private ZkConfig() {
    }

    public synchronized static ZkConfig instance() {
        return new ZkConfig();
    }

    public void initZk() {
        try {
            JSONObject jsonObject = new ZookeeperLoader().loadConfig();
            new ZookeeperSaver().saveConfig(jsonObject);
        } catch (Exception e) {
            LOGGER.error("fail to load configuration form zookeeper,using local file to run!", e);
        }
    }
}
