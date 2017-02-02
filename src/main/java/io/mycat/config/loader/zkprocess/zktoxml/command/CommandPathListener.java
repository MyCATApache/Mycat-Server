package io.mycat.config.loader.zkprocess.zktoxml.command;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.mycat.MycatServer;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.console.ZkNofiflyCfg;
import io.mycat.config.loader.zkprocess.zktoxml.ZktoXmlMain;
import io.mycat.manager.handler.ZKHandler;
import io.mycat.manager.response.ReloadConfig;
import io.mycat.net.NIOProcessor;
import io.mycat.util.ZKUtils;

/**
 * zk命令监听器
 * @author kk
 * @date 2017年1月18日
 * @version 0.0.1
 */
public class CommandPathListener implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandPathListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
        case CHILD_ADDED:
            // 在发生节点添加的时候，则执行接收命令并执行
            // 1,首先检查
            String path = event.getData().getPath();
            String basePath = ZKUtils.getZKBasePath() + ZKHandler.ZK_NODE_PATH + "/";

            // 检查节点与当前的节点是否一致
            String node = path.substring(basePath.length());

            if (node.equals(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID))) {
                // 检查命令内容是否为
                if (ZKHandler.RELOAD_FROM_ZK.equals(new String(client.getData().forPath(path)))) {
                    // 从服务器上下载最新的配制文件信息
                    ZktoXmlMain.ZKLISTENER.notifly(ZkNofiflyCfg.ZK_NOTIFLY_LOAD_ALL.getKey());
                    // 重新加载配制信息
                    reload(path);
                    // 完成之后，删除命令信息， 以供下次读取
                    client.delete().forPath(event.getData().getPath());
                    LOGGER.info("CommandPathListener path:" + path + " reload success");
                }
            }

            break;
        case CHILD_UPDATED:
            break;
        case CHILD_REMOVED:
            break;
        default:
            break;
        }

    }

    public void reload(final String path) {
        // reload @@config_all 校验前一次的事务完成情况
        if (!NIOProcessor.backends_old.isEmpty()) {
            return;
        }

        final ReentrantLock lock = MycatServer.getInstance().getConfig().getLock();
        lock.lock();
        try {
            ListenableFuture<Boolean> listenableFuture = MycatServer.getInstance().getListeningExecutorService()
                    .submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            return ReloadConfig.reload_all();
                        }
                    });
            Futures.addCallback(listenableFuture, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    LOGGER.info("CommandPathListener path:" + path + " reload success");
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.error("CommandPathListener path:" + path + " reload error", t);
                }

            }, MycatServer.getInstance().getListeningExecutorService());
        } finally {
            lock.unlock();
        }
    }

}
