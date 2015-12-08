package org.opencloudb.config;

import org.json.JSONObject;
import org.opencloudb.config.loader.zookeeper.ZookeeperLoader;
import org.opencloudb.config.loader.zookeeper.ZookeeperSaver;

public class ZkConfig {

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
            e.printStackTrace();
        }
    }
}
