package org.opencloudb.config;

import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.opencloudb.config.loader.zookeeper.ZookeeperLoader;
import org.opencloudb.config.loader.zookeeper.ZookeeperSaver;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ZkConfig {
    private static final Logger LOGGER = Logger.getLogger(ZkConfig.class);
    private static final String ZK_CONFIG_FILE_NAME = "/myid.properties";

    private ZkConfig() {
    }

    public synchronized static ZkConfig instance() {
        return new ZkConfig();
    }

    public void initZk() {
        Properties pros = loadMyid();

        //disable load from zookeeper,use local file.
        if (pros == null) {
            LOGGER.trace("use local configuration to startup");
            return;
        }
        
        try {
            JSONObject jsonObject = new ZookeeperLoader().loadConfig(pros);
            new ZookeeperSaver().saveConfig(jsonObject);
            LOGGER.trace("use zookeeper configuration to startup");
        } catch (Exception e) {
            LOGGER.error("fail to load configuration form zookeeper,using local file to run!", e);
        }
    }

    public Properties loadMyid() {
        Properties pros = new Properties();

        try (InputStream configIS = ZookeeperLoader.class
            .getResourceAsStream(ZK_CONFIG_FILE_NAME)) {
            if (configIS == null) {
                //file is not exist ,so ues local file.
                return null;
            }

            pros.load(configIS);
        } catch (IOException e) {
            throw new RuntimeException("can't find myid properties file : " + ZK_CONFIG_FILE_NAME);
        }

        if (Boolean.valueOf(pros.getProperty("loadZk"))) {
            //validate
            String zkURL = pros.getProperty("zkURL");
            String myid = pros.getProperty("myid");

            if (Strings.isNullOrEmpty(zkURL) || Strings.isNullOrEmpty(myid)) {
                throw new RuntimeException("zkURL and myid must be not null or empty!");
            }
            return pros;
        }

        return null;
    }

}
