package org.opencloudb.config.loader.zookeeper.loader;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.json.JSONObject;


public class NodesLoader extends AbstractLoader {
    protected static final String NODES_PATH = "mycat-nodes";

    public NodesLoader(CuratorFramework curator) {
        super(curator);
    }

    //scan the path under /mycat/mycat-nodes/${myid}
    @Override public JSONObject takeConfig(String myid) throws Exception {
        String nodesRootPath = ZKPaths.makePath("/", NODES_PATH, myid);
        return new JSONObject(getDataToString(nodesRootPath));
    }
}
