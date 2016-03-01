package io.mycat.config;

import org.junit.Ignore;
import org.junit.Test;

import io.mycat.config.ZkConfig;

public class ZkConfigTest extends ZookeeperTestServer {

    @Test @Ignore public void testInstance() throws Exception {
        ZkConfig zkConfig = ZkConfig.instance();
        zkConfig.initZk();
    }
}
