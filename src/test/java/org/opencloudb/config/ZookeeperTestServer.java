package org.opencloudb.config;

import demo.catlets.ZkCreate;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;

/**
 * Created by lion on 12/8/15.
 */
@Ignore
public class ZookeeperTestServer {

    protected static TestingServer testingServer;

    @BeforeClass public static void setUpZookeeper() throws Exception {
        testingServer = new TestingServer(true);
        ZkCreate.main(new String[] {"/zk-create-test.yaml", testingServer.getConnectString()});
    }

    @AfterClass public static void tearDown() throws IOException {
        testingServer.close();
    }
}
