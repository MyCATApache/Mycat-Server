package io.mycat.locator;

import io.mycat.server.config.ConfigException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * ServiceLocator zookeeper implements.
 * <p>responsible for connecting zookeeper server and provide </p>
 * Created by v1.lion on 2015/10/5.
 */
public class ZookeeperServiceLocator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperServiceLocator.class);

    private ZookeeperServiceLocator() {
        super();
    }

    public static CuratorFramework createConnection(String connectString) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .newClient(connectString, new ExponentialBackoffRetry(100, 6));

        //start connection
        curatorFramework.start();
        LOGGER.debug("connect to zookeeper server : {}", connectString);

        //wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework;
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

        //fail situation
        curatorFramework.close();
        throw new ConfigException("failed to connect to zookeeper service : " + connectString);
    }
}
