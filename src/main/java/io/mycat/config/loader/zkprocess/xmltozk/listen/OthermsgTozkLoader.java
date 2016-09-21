package io.mycat.config.loader.zkprocess.xmltozk.listen;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

/**
 * 其他一些信息加载到zk中
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class OthermsgTozkLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(OthermsgTozkLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    public OthermsgTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 添加line目录，用作集群中节点，在线的基本目录信息
        String line = currZkPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_LINE.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), line);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + line + " success");

        // 添加序列目录信息
        String seqLine = currZkPath + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();
        seqLine = seqLine + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_INSTANCE.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), seqLine);

        String seqLeader = currZkPath + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();
        seqLeader = seqLeader + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_LEADER.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), seqLeader);

        String incrSeq = currZkPath + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();
        incrSeq = incrSeq + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_INCREMENT_SEQ.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), incrSeq);

        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + seqLine + " success");
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + seqLeader + " success");
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + incrSeq + " success");

        return true;
    }

}
