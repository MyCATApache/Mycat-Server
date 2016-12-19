package io.mycat.migrate;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

/**
 * Created by magicdoom on 2016/12/19.
 */
public class SwitchListener implements PathChildrenCacheListener {
    @Override public void childEvent(CuratorFramework curatorFramework,
            PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED:

                break;
            default:
                break;
        }
    }
}
