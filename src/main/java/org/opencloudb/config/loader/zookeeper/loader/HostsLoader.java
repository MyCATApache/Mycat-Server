package org.opencloudb.config.loader.zookeeper.loader;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.json.JSONObject;


public class HostsLoader extends AbstractLoader {
    private static final String HOSTS_PATH = "mycat-hosts";

    public HostsLoader(CuratorFramework curator) {
        super(curator);
    }

    //scan the path under /mycat/mycat-hosts
    @Override public JSONObject takeConfig(String path) throws Exception {
        final String hostsRootPath = ZKPaths.makePath("/", HOSTS_PATH);
        return takeData(hostsRootPath);
    }
}
