package org.opencloudb.config;

import org.junit.Ignore;
import org.junit.Test;

public class ZkConfigTest extends ZookeeperTestServer {

    @Test @Ignore public void testInstance() throws Exception {
        ZkConfig zkConfig = ZkConfig.instance();
        zkConfig.initZk();
    }
}
