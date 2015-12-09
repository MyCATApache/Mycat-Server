package org.opencloudb.config.loader.zookeeper;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.opencloudb.config.ZookeeperTestServer;
import org.opencloudb.config.loader.zookeeper.entitiy.Property;
import org.opencloudb.config.loader.zookeeper.entitiy.Server;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.IsCollectionContaining.hasItems;

/**
 * Created by lion on 12/8/15.
 */
public class ZookeeperSaverTest extends ZookeeperTestServer {
    private JSONObject config;

//    @Before public void setUp() throws Exception {
//        ZookeeperLoader loader = new ZookeeperLoader();
//        loader.setZkURl(testingServer.getConnectString());
//        config = loader.loadConfig();
//    }
//
//    @Test public void testSaveServer() throws Exception {
//
//        ZookeeperSaver saver = new ZookeeperSaver();
//        Server server = saver.saveServer(config);
//
//        assertThat(server.getSystem().getProperty().size(), is(3));
//        assertThat(server.getSystem().getProperty().get(0),
//            is(new Property().setName("serverport").setValue("8066")));
//        assertThat(server.getSystem().getProperty().get(1),
//            is(new Property().setName("sequncehandlertype").setValue("1")));
//        assertThat(server.getSystem().getProperty().get(2),
//            is(new Property().setName("defaultsqlparser").setValue("druidparser")));
//
//        //users
//        assertThat(server.getUser().size(), is(2));
//
//        Server.User expectTest = new Server.User();
//        Property expectTestP1 = new Property().setName("readOnly").setValue("true");
//        Property expectTestP2 = new Property().setName("name").setValue("test");
//        Property expectTestP3 = new Property().setName("password").setValue("admin");
//        Property expectTestP4 = new Property().setName("schemas").setValue("testdb,test");
//        expectTest.addProperty(expectTestP1);
//        expectTest.addProperty(expectTestP2);
//        expectTest.addProperty(expectTestP3);
//        expectTest.addProperty(expectTestP4);
//
//        assertThat(server.getUser().get(0).getProperty(),
//            hasItems(expectTestP1, expectTestP2, expectTestP3, expectTestP4));
//
//        Server.User expectMycat = new Server.User();
//        Property expectMycatP1 = new Property().setName("readOnly").setValue("false");
//        Property expectMycatP2 = new Property().setName("name").setValue("mycat");
//        Property expectMycatP3 = new Property().setName("password").setValue("admin");
//        Property expectMycatP4 = new Property().setName("schemas").setValue("testdb");
//        expectMycat.addProperty(expectMycatP1);
//        expectMycat.addProperty(expectMycatP2);
//        expectMycat.addProperty(expectMycatP3);
//        expectMycat.addProperty(expectMycatP4);
//        assertThat(server.getUser().get(1).getProperty(),
//            hasItems(expectMycatP1, expectMycatP2, expectMycatP3, expectMycatP4));
//    }
}
