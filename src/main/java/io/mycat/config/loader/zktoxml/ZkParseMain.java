package io.mycat.config.loader.zktoxml;

import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zktoxml.ZkConfig.ZkParamCfg;
import io.mycat.config.loader.zktoxml.comm.XmlProcessBase;
import io.mycat.config.loader.zktoxml.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zktoxml.console.ZkNofiflyCfg;
import io.mycat.config.loader.zktoxml.listen.SchemasLoader;

public class ZkParseMain {

    public static void main(String[] args) throws JAXBException {
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        // 得到集群名称
        String custerName = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID);
        // 得到基本路径
        String basePath = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_BASE.getKey();
        basePath = basePath + ZookeeperPath.ZK_SEPARATOR.getKey() + custerName;
        zkListen.setBasePath(basePath);

        // 获得zk的连接信息
        CuratorFramework zkConn = buildConnection(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_URL));

        // 获得公共的xml转换器对象
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // 加载以接收者
        new SchemasLoader(zkListen, zkConn, xmlProcess);

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 通知所有人
        zkListen.notifly(ZkNofiflyCfg.ZK_NOTIFLY_LOAD_ALL.getKey());
    }

    private static CuratorFramework buildConnection(String url) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(url, new ExponentialBackoffRetry(100, 6));

        // start connection
        curatorFramework.start();
        // wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework.usingNamespace("");
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        // fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }
}
