package io.mycat.server.config.loader.zkloader;

import org.junit.Test;

/**
 * Created by v1.lion on 2015/10/5.
 */
public class ZookeeperLoaderTest {
    @Test
    public void testConstruct() {
        ZookeeperLoader zookeeperLoader = new ZookeeperLoader();
        zookeeperLoader.initConfig();
    }
}