package io.mycat.config.loader.zkprocess.zktoxml;

import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.MycatConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.console.ZkNofiflyCfg;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zktoxml.listen.EcacheszkToxmlLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.RuleszkToxmlLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.SchemaszkToxmlLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.SequenceTopropertiesLoader;
import io.mycat.config.loader.zkprocess.zktoxml.listen.ServerzkToxmlLoader;

public class ZktoxmlMain {

    public static void main(String[] args) throws JAXBException, InterruptedException {
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        // 得到集群名称
        String custerName = MycatConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID);
        // 得到基本路径
        String basePath = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_BASE.getKey();
        basePath = basePath + ZookeeperPath.ZK_SEPARATOR.getKey() + custerName;
        zkListen.setBasePath(basePath);

        // 获得zk的连接信息
        CuratorFramework zkConn = buildConnection(MycatConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_URL));

        // 获得公共的xml转换器对象
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // 加载以接收者
        new SchemaszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // server加载
        new ServerzkToxmlLoader(zkListen, zkConn, xmlProcess);

        // rule文件加载
        new RuleszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // 将序列配制信息加载
        new SequenceTopropertiesLoader(zkListen, zkConn, xmlProcess);

        // 进行ehcache转换
        new EcacheszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 通知所有人
        zkListen.notifly(ZkNofiflyCfg.ZK_NOTIFLY_LOAD_ALL.getKey());

        // Thread.currentThread().sleep(1000000);
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
