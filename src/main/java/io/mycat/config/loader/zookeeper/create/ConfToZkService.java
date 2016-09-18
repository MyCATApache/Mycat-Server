package io.mycat.config.loader.zookeeper.create;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import io.mycat.config.loader.zookeeper.ZkCreate;
import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;
import io.mycat.config.loader.zookeeper.create.comm.ZkProcessBase;
import io.mycat.config.loader.zookeeper.create.flow.FlowToBinDataService;
import io.mycat.config.loader.zookeeper.create.flow.FlowToCacheService;
import io.mycat.config.loader.zookeeper.create.flow.FlowToLineService;
import io.mycat.config.loader.zookeeper.create.flow.FlowToNodeService;
import io.mycat.config.loader.zookeeper.create.flow.FlowToRuleService;
import io.mycat.config.loader.zookeeper.create.flow.FlowToSchemaService;
import io.mycat.config.loader.zookeeper.create.flow.FlowToSequenceService;
import io.mycat.config.loader.zookeeper.create.flow.FlowToServerService;

/**
 * zk操作服务信息
* 源文件名：ConfToZkService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ConfToZkService {
    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkProcessBase.class);

    /**
     * zk导入的默认的文件的名称
    * @字段说明 ZK_CONFIG_FILE_NAME
    */
    private static String ZK_CFG_DEF_FILE_NAME = "/zk-create-2.yaml";

    /**
     * yaml文件中配制的zk路径
    * @字段说明 CONFIG_URL_KEY
    */
    private static final String CONFIG_URL_KEY = "zkURL";

    /**
     * zk公共操作类
    * @字段说明 zkProcessBase
    */
    private ZkProcessBase zkProcessBase = new ZkProcessBase();

    /**
     * 从配制文件zk-create.yaml至zk的流程
    * @字段说明 FLOWSERVICE
    */
    private static ServiceExecInf[] FLOWSERVICE = new ServiceExecInf[8];

    static {
        // 流程配制信息
        // 创建集群中在线的节点的目录
        FLOWSERVICE[0] = new FlowToLineService();
        // schema文件至zk操作
        FLOWSERVICE[1] = new FlowToSchemaService();
        // 路由信息的至zk操作
        FLOWSERVICE[2] = new FlowToRuleService();
        // 服务端信息至zk操作
        FLOWSERVICE[3] = new FlowToServerService();
        // 序列至zk操作
        FLOWSERVICE[4] = new FlowToSequenceService();
        // 缓存信息至zk操作
        FLOWSERVICE[5] = new FlowToCacheService();
        // 节点状态等信息至zk
        FLOWSERVICE[6] = new FlowToBinDataService();
        // 单独节点的配制信息
        FLOWSERVICE[7] = new FlowToNodeService();
    }

    /**
     * 创建与zk的连接
    * 方法描述
    * @param url
    * @return
    * @创建日期 2016年9月11日
    */
    private CuratorFramework createConnection(String url) {
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

    /**
     * 加载配制的yaml文件的信息
    * 方法描述
    * @return
    * @创建日期 2016年9月11日
    */
    @SuppressWarnings("unchecked")
    private Map<String, Object> loadZkConfig(String zkCfgFileName) {
        // 如果未设计使用默认的文件名
        if (null == zkCfgFileName || "".equals(zkCfgFileName)) {
            zkCfgFileName = ZK_CFG_DEF_FILE_NAME;
        }

        InputStream configIS = ZkCreate.class.getResourceAsStream(zkCfgFileName);
        if (configIS == null) {
            throw new RuntimeException("can't find zk properties file : " + zkCfgFileName);
        }
        return (Map<String, Object>) new Yaml().load(configIS);
    }

    /**
     * 写入zk信息
    * 方法描述
    * @param fileName
    * @param zkUrl
    * @创建日期 2016年9月11日
    */
    public void writeToZk(String fileName, String zkUrl) {

        // 加载文件信息
        Map<String, Object> map = this.loadZkConfig(fileName);

        // 在配制文件中配制的zk地址信息
        if (null == zkUrl || "".equals(zkUrl)) {
            zkUrl = map.containsKey(CONFIG_URL_KEY) ? (String) map.get(CONFIG_URL_KEY) : "127.0.0.1:2181";
        }

        // 加载zk信息
        CuratorFramework zkClient = this.createConnection(zkUrl);

        zkProcessBase.setFramework(zkClient);
        zkProcessBase.setZkConfig(map);

        SeqLinkedList flowList = new SeqLinkedList();

        // 设定执行流程
        flowList.addExec(FLOWSERVICE);
        // 设置zk公共处理类信息
        flowList.setZkProcess(zkProcessBase);

        // 进行流程的执行
        try {
            flowList.nextExec();
        } catch (Exception e) {
            LOGGER.error("ConfToZkService exception error", e);
        }
    }

}
