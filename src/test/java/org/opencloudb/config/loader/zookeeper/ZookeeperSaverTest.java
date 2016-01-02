package org.opencloudb.config.loader.zookeeper;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencloudb.config.ZkConfig;
import org.opencloudb.config.ZookeeperTestServer;
import org.opencloudb.config.loader.zookeeper.entitiy.Property;
import org.opencloudb.config.loader.zookeeper.entitiy.Rules;
import org.opencloudb.config.loader.zookeeper.entitiy.Schemas;
import org.opencloudb.config.loader.zookeeper.entitiy.Server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Created by lion on 12/8/15.
 */
@Ignore
public class ZookeeperSaverTest extends ZookeeperTestServer {
    private JSONObject config;

    @Before public void setUp() throws Exception {
        ZookeeperLoader loader = new ZookeeperLoader();
        loader.setZkURl(testingServer.getConnectString());
        config = loader.loadConfig(ZkConfig.instance().loadMyid());
    }

    private List<Property> getExpectSystemProperty() {
        List<Property> system = new ArrayList<>();
        system.add(new Property().setName("serverport").setValue("8066"));
        system.add(new Property().setName("sequncehandlertype").setValue("1"));
        system.add(new Property().setName("defaultsqlparser").setValue("druidparser"));
        return system;
    }


    private Map<String, Server.User> getExpectUser() {
        Map<String, Server.User> map = new HashMap<>();

        Server.User test = new Server.User();
        test.setName("test");
        test.addProperty(new Property().setName("readOnly").setValue("true"));
        test.addProperty(new Property().setName("password").setValue("admin"));
        test.addProperty(new Property().setName("schemas").setValue("testdb,test"));

        Server.User mycat = new Server.User();
        mycat.setName("mycat");
        mycat.addProperty(new Property().setName("readOnly").setValue("false"));
        mycat.addProperty(new Property().setName("password").setValue("admin"));
        mycat.addProperty(new Property().setName("schemas").setValue("testdb"));

        map.put("test", test);
        map.put("mycat", mycat);
        return map;
    }


    @Test public void testSaveServer() throws Exception {
        List<Property> expectSystemProperty = getExpectSystemProperty();
        Map<String, Server.User> expectUsers = getExpectUser();

        ZookeeperSaver saver = new ZookeeperSaver();
        Server server = saver.saveServer(config, "server_zk");

        assertThat(server.getSystem().getProperty().containsAll(expectSystemProperty),
            is(Boolean.TRUE));

        //users
        assertThat(server.getUser().size(), is(2));
        for (Server.User user : server.getUser()) {
            Server.User expectUser = expectUsers.get(user.getName());

            assertNotNull(expectUser);
            assertThat(user.getName(), is(expectUser.getName()));
            assertThat(user.getProperty().containsAll(expectUser.getProperty()), is(Boolean.TRUE));
        }
    }

    private Map<String, Rules.TableRule> getExpectRule() {
        Map<String, Rules.TableRule> map = new HashMap<>();

        Rules.TableRule shardingByEnum = new Rules.TableRule();
        shardingByEnum.setName("sharding-by-enum");

        Rules.TableRule.Rule shardingByEnumRule = new Rules.TableRule.Rule();
        shardingByEnumRule.setColumns("create_time");
        shardingByEnumRule.setAlgorithm("io.mycat.route.function.PartitionByFileMap");
        shardingByEnum.setRule(shardingByEnumRule);

        map.put("sharding-by-enum", shardingByEnum);
        return map;
    }

    private Map<String, Rules.Function> getExpectFunction() {
        return null;
    }

    @Test public void testSaveRule() throws Exception {
        Map<String, Rules.TableRule> expectRule = getExpectRule();

        ZookeeperSaver saver = new ZookeeperSaver();
        Rules rules = saver.saveRule(config, "rule_zk");

        //        rules.getTableRule();

    }

    @Test public void testSaveSchema() throws Exception {
        Map<String, Rules.TableRule> expectRule = getExpectRule();

        ZookeeperSaver saver = new ZookeeperSaver();
        Schemas schemas = saver.saveSchema(config, "schema_zk");

        //        rules.getTableRule();

    }
}

