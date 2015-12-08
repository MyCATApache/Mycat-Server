package org.opencloudb.config.loader.zookeeper.loader;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.json.JSONObject;


public class MysqlsLoader extends AbstractLoader {
    private static final String MYSQLS_PATH = "mycat-mysqls";

    public MysqlsLoader(CuratorFramework curator) {
        super(curator);
    }

    //scan the path under /mycat/mycat-mysqls
    @Override public JSONObject takeConfig(String path) throws Exception {
        return takeData(ZKPaths.makePath("/", MYSQLS_PATH));
    }
}
