package org.opencloudb.config.loader.zookeeper.loader;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.json.JSONObject;


public class ClusterLoader extends AbstractLoader {
    private static final String CLUSTER_PATH = "mycat-cluster";

    public ClusterLoader(CuratorFramework curator) {
        super(curator);
    }

    //scan the path under /mycat/mycat-cluster/${myCluster}
    @Override public JSONObject takeConfig(String path) throws Exception {
        return takeData(ZKPaths.makePath("/", CLUSTER_PATH, path));
    }
}
