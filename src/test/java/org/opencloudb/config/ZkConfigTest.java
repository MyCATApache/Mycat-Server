package org.opencloudb.config;

import org.junit.Test;

public class ZkConfigTest extends ZookeeperTestServer {

    @Test public void testInstance() throws Exception {
        ZkConfig zkConfig = ZkConfig.instance();
        zkConfig.initZk();
    }
}
