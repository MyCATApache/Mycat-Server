package org.opencloudb.config.loader.zookeeper.loader;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.json.JSONObject;


public class MysqlGroupLoader extends AbstractLoader {
    private static final String MYSQL_GROUP_PATH = "mycat-mysqlgroup";

    public MysqlGroupLoader(CuratorFramework curator) {
        super(curator);
    }

    //scan the path under /mycat/mycat-mysqlgroup
    @Override public JSONObject takeConfig(String path) throws Exception {
        return takeData(ZKPaths.makePath("/", MYSQL_GROUP_PATH));
    }
}
