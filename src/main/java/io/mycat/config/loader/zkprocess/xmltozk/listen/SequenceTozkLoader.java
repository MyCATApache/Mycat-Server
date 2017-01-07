package io.mycat.config.loader.zkprocess.xmltozk.listen;

import java.io.IOException;
import java.io.InputStream;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

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
public class SequenceTozkLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTozkLoader.class);

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

    public SequenceTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

    }

    @Override
    public boolean notiflyProcess() throws Exception {

        // 将zk序列配配制信息入zk
        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_CONF);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_conf to zk success");

        // 将zk的db方式信息入zk
        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_DB_CONF);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_db_conf to zk success");

        // 将zk的分布式信息入zk
        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_DISTRIBUTED_CONF);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_distributed_conf to zk success");

        // 将时间序列入zk
        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_TIME_CONF);

        LOGGER.info("SequenceTozkLoader notiflyProcess sequence_time_conf to zk success");

        LOGGER.info("SequenceTozkLoader notiflyProcess xml to zk is success");

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
    private void sequenceTozk(String basePath, String name) throws Exception {
        // 读取当前节的信息
        String commPath = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey()
                + ZookeeperPath.ZK_SEPARATOR.getKey();

        String readFile = name + PROPERTIES_SUFFIX;
        // 读取公共节点的信息
        String commSequence = this.readSequenceCfg(readFile);
        String sequenceZkPath = commPath + readFile;
        this.checkAndwriteString(basePath, sequenceZkPath, commSequence);

        // 集群中特有的节点的配制信息
        String culsterPath = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey()
                + ZookeeperPath.ZK_SEPARATOR.getKey();

        String[] clusters = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTER_NODES)
                .split(ZkParamCfg.ZK_CFG_CLUSTER_NODES_SEPARATE.getKey());

        if (null != clusters) {
            String nodeName = null;
            for (String clusterName : clusters) {
                nodeName = name + "-" + clusterName + PROPERTIES_SUFFIX;
                // 读取当前集群中特有的节点的信息
                String clusterSequence = this.readSequenceCfg(nodeName);

                // 如果配制了特定节点的信息,则将往上入zk中
                if (null != clusterSequence) {
                    String seqclusterZkPath = culsterPath + nodeName;
                    this.checkAndwriteString(basePath, seqclusterZkPath, clusterSequence);
                }
            }

        }
    }

    /**
     * 读取 sequence配制文件的信息
    * 方法描述
    * @param name 名称信息
    * @return
    * @创建日期 2016年9月18日
    */
    private String readSequenceCfg(String name) {

        String path = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + name;
        // 加载数据
        InputStream input = SequenceTozkLoader.class.getResourceAsStream(path);

        if (null != input) {

            StringBuilder mapFileStr = new StringBuilder();

            byte[] buffers = new byte[256];

            try {
                int readIndex = -1;

                while ((readIndex = input.read(buffers)) != -1) {
                    mapFileStr.append(new String(buffers, 0, readIndex));
                }
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.error("SequenceTozkLoader readMapFile IOException", e);

            } finally {
                IOUtils.close(input);
            }

            return mapFileStr.toString();
        }
        return null;
    }

}
