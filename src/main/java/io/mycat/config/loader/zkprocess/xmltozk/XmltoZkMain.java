package io.mycat.config.loader.zkprocess.xmltozk;

import javax.xml.bind.JAXBException;

import com.alibaba.fastjson.JSON;
import io.mycat.config.loader.zkprocess.zookeeper.ClusterInfo;
import org.apache.curator.framework.CuratorFramework;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.console.ZkNofiflyCfg;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.xmltozk.listen.EcachesxmlTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.OthermsgTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.RulesxmlTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.SchemasxmlTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.SequenceTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.ServerxmlTozkLoader;
import io.mycat.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XmltoZkMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmltoZkMain.class);
    public static void main(String[] args) throws JAXBException, InterruptedException {
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

        // 进行xmltozk的schema文件的操作
        new SchemasxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行xmltozk的server文件的操作
        new ServerxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行rule文件到zk的操作
        new RulesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行序列信息入zk中
        new SequenceTozkLoader(zkListen, zkConn, xmlProcess);

        // 缓存配制信息
        new EcachesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 将其他信息加载的zk中
        new OthermsgTozkLoader(zkListen, zkConn, xmlProcess);

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();


        // 加载通知进程
        zkListen.notifly(ZkNofiflyCfg.ZK_NOTIFLY_LOAD_ALL.getKey());



        String clusterNodes=    ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTER_NODES);
        String clusterSize=    ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTER_SIZE);
        ClusterInfo info=new ClusterInfo();
        info.setClusterNodes(clusterNodes);
        info.setClusterSize(Integer.parseInt(clusterSize));
        try {
            zkConn.setData().forPath(basePath, JSON.toJSONBytes(info));
        } catch (Exception e) {
            LOGGER.error("error",e);
        }

    }

    private static CuratorFramework buildConnection(String url) {

        return ZKUtils.getConnection();
    }
}
