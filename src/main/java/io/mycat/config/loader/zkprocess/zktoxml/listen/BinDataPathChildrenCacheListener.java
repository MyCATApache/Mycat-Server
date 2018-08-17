package io.mycat.config.loader.zkprocess.zktoxml.listen;

import com.google.common.io.Files;
import io.mycat.MycatServer;
import io.mycat.config.model.SystemConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.io.File;
import java.io.IOException;

/**
 * bindata目录监听器
 * zk的 /mycat/clusterId/bindata下有变化回调
 * Created by magicdoom on 2016/10/27.
 */
public class BinDataPathChildrenCacheListener implements PathChildrenCacheListener {
    @Override public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData data = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED: // zk向 bindata 添加节点
                add(data.getPath().substring(data.getPath().lastIndexOf("/")+1),event.getData().getData()) ;
                break;
            case CHILD_REMOVED: // zk向 bindata 删除节点
                delete(data.getPath().substring(data.getPath().lastIndexOf("/")+1),event.getData().getData()); ;
                break;
            case CHILD_UPDATED: // zk向 bindata 更新节点
                add(data.getPath().substring(data.getPath().lastIndexOf("/")+1),event.getData().getData()) ;
                break;
            default:
                break;
        }
    }

    private void add(String name,byte[] data) throws IOException {
        File file = new File(
                SystemConfig.getHomePath() + File.separator + "conf" ,
                name);
        Files.write(data,file);
        //try to reload dnindex
        if("dnindex.properties".equals(name)) {
            MycatServer.getInstance().reloadDnIndex();
        }
    }

    private void delete(String name,byte[] data) throws IOException {
        File file = new File(
                SystemConfig.getHomePath() + File.separator + "conf" ,
                name);
        if(file.exists()) {
            file.delete();
        }
    }

}
