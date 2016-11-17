package io.mycat.config.loader.zkprocess.zktoxml.listen;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.mycat.MycatServer;
import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ReloadConfig;

/**
 * 进行从sequence加载到zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SequenceTopropertiesLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTopropertiesLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * 后缀名
    * @字段说明 PROPERTIES_SUFFIX
    */
    private static final String PROPERTIES_SUFFIX = ".properties";

    /**
     * 序列配制信息
    * @字段说明 PROPERTIES_SEQUENCE_CONF
    */
    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    /**
     * db序列配制信息
     * @字段说明 PROPERTIES_SEQUENCE_CONF
     */
    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";

    /**
     * 分布式的序列配制
     * @字段说明 PROPERTIES_SEQUENCE_CONF
     */
    private static final String PROPERTIES_SEQUENCE_DISTRIBUTED_CONF = "sequence_distributed_conf";

    /**
     * 时间的序列配制
     * @字段说明 PROPERTIES_SEQUENCE_CONF
     */
    private static final String PROPERTIES_SEQUENCE_TIME_CONF = "sequence_time_conf";

    /**
     * 监控路径信息
    * @字段说明 zookeeperListen
    */
    private ZookeeperProcessListen zookeeperListen;

    public SequenceTopropertiesLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        this.zookeeperListen = zookeeperListen;

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        this.zookeeperListen.addListen(schemaPath, this);

    }

    @Override
    public boolean notiflyProcess() throws Exception {

        // 1,将集群server目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf sequenceDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey(), sequenceDirectory);

        // 取到当前根目录 信息
        sequenceDirectory = (DiretoryInf) sequenceDirectory.getSubordinateInfo().get(0);

        // 将zk序列配配制信息入本地文件
        this.sequenceZkToProperties(currZkPath, PROPERTIES_SEQUENCE_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_conf to local properties success");

        // 将zk的db方式信息入本地文件
        this.sequenceZkToProperties(currZkPath, PROPERTIES_SEQUENCE_DB_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_db_conf to local properties success");

        // 将zk的分布式信息入本地文件
        this.seqWriteOneZkToProperties(currZkPath, PROPERTIES_SEQUENCE_DISTRIBUTED_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_distributed_conf to local properties success");

        // 将zk时间序列入本地文件
        this.seqWriteOneZkToProperties(currZkPath, PROPERTIES_SEQUENCE_TIME_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_time_conf to local properties success");

        LOGGER.info("SequenceTozkLoader notiflyProcess xml to local properties is success");

        if (MycatServer.getInstance().getProcessors() != null)
            ReloadConfig.reload();
        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
    * 方法描述
    * @param basePath 基本路径
    * @param schema schema文件的信息
    * @throws Exception 异常信息
    * @创建日期 2016年9月17日
    */
    private void sequenceZkToProperties(String basePath, String name, DiretoryInf seqDirectory) throws Exception {
        // 读取当前节的信息
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey());

        if (null != zkDirectory) {
            String writeFile = name + PROPERTIES_SUFFIX;

            // 读取common目录下的数据
            ZkDataImpl commData = (ZkDataImpl) this.getZkData(zkDirectory, writeFile);

            // 读取公共节点的信息
            this.writeMapFile(commData.getName(), commData.getValue());

            String seqComm = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey();
            seqComm = seqComm + ZookeeperPath.ZK_SEPARATOR.getKey() + commData.getName();

            this.zookeeperListen.watchPath(currZkPath, seqComm);

        }

        // 集群中特有的节点的配制信息
        ZkDirectoryImpl zkClusterDir = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey());

        if (null != zkClusterDir) {

            String clusterName = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);

            String nodeName = name + "-" + clusterName + PROPERTIES_SUFFIX;

            // 读取cluster目录下的数据
            ZkDataImpl clusterData = (ZkDataImpl) this.getZkData(zkClusterDir, nodeName);

            if (null != clusterData) {
                // 读取当前集群中特有的节点的信息
                this.writeMapFile(clusterData.getName(), clusterData.getValue());

                String seqCluster = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey();
                seqCluster = seqCluster + ZookeeperPath.ZK_SEPARATOR.getKey() + clusterData.getName();

                this.zookeeperListen.watchPath(currZkPath, seqCluster);
            }
        }
    }

    /**
     * 将xml文件的信息写入到zk中
     * 方法描述
     * @param basePath 基本路径
     * @param schema schema文件的信息
     * @throws Exception 异常信息
     * @创建日期 2016年9月17日
     */
    private void seqWriteOneZkToProperties(String basePath, String name, DiretoryInf seqDirectory) throws Exception {
        // 读取当前节的信息
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey());

        ZkDataImpl commData = null;

        if (null != zkDirectory) {
            String writeFile = name + PROPERTIES_SUFFIX;

            // 读取common目录下的数据
            commData = (ZkDataImpl) this.getZkData(zkDirectory, writeFile);

            // comm路径的监控路径
            String seqComm = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey();
            seqComm = seqComm + ZookeeperPath.ZK_SEPARATOR.getKey() + commData.getName();

            this.zookeeperListen.watchPath(currZkPath, seqComm);
        }

        // 集群中特有的节点的配制信息
        ZkDirectoryImpl zkClusterDir = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey());

        ZkDataImpl clusterData = null;

        if (null != zkClusterDir) {

            String clusterName = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);

            String nodeName = name + "-" + clusterName + PROPERTIES_SUFFIX;

            // 读取cluster目录下的数据
            clusterData = (ZkDataImpl) this.getZkData(zkClusterDir, nodeName);

            if (null != clusterData) {
                // comm路径的监控路径
                String seqCluster = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey();
                seqCluster = seqCluster + ZookeeperPath.ZK_SEPARATOR.getKey() + clusterData.getName();

                this.zookeeperListen.watchPath(currZkPath, seqCluster);
            }
        }

        // 如果配制了单独节点的信息,以公共的名称，写入当前的值
        if (clusterData != null && commData != null) {
            // 读取公共节点的信息
            this.writeMapFile(commData.getName(), clusterData.getValue());
        } else if (commData != null) {
            // 读取当前集群中特有的节点的信息
            this.writeMapFile(commData.getName(), commData.getValue());
        }
    }

    /**
     * 读取 mapFile文件的信息
    * 方法描述
    * @param name 名称信息
    * @return
    * @创建日期 2016年9月18日
    */
    private void writeMapFile(String name, String value) {

        // 加载数据
        String path = RuleszkToxmlLoader.class.getClassLoader().getResource(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey())
                .getPath();

        checkNotNull(path, "write Map file curr Path :" + path + " is null! must is not null");

        path = new File(path).getPath() + File.separator;
        path += name;

        // 进行数据写入
        try {
            Files.write(value.getBytes(), new File(path));
        } catch (IOException e1) {
            e1.printStackTrace();
        }

    }

}
