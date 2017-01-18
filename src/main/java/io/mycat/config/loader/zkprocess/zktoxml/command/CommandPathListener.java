package io.mycat.config.loader.zkprocess.zktoxml.command;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

/**
 * zk命令监听器
 * @author kk
 * @date 2017年1月18日
 * @version 0.0.1
 */
public class CommandPathListener implements PathChildrenCacheListener {

    /**
     * 监听器对象
     */
    private PathChildrenCache cache;

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
        case CHILD_ADDED:
            // 在发生节点添加的时候，则执行接收命令并执行
            // 1,首先检查
            event.getData();

            System.out.println(event);

            break;
        case CHILD_UPDATED:
            System.out.println(event);
            
            
            client.delete().forPath(event.getData().getPath());
            break;
        case CHILD_REMOVED:
            break;
        default:
            break;
        }

    }

    public void setCache(PathChildrenCache cache) {
        this.cache = cache;
    }

}
