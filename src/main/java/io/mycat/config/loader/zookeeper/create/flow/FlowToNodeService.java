package io.mycat.config.loader.zookeeper.create.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;
import io.mycat.config.loader.zookeeper.create.console.FlowCfg;
import io.mycat.config.loader.zookeeper.create.console.SysFlow;

/**
 * 将节点的单独的配制信息录入到zk中
* 源文件名：FlowToNodeService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FlowToNodeService implements ServiceExecInf {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowToNodeService.class);

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制cache的配制信息
        String writeNodePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FLOW_ZK_PATH_NODE.getKey()
                + SysFlow.ZK_SEPARATOR;

        // map获取路径 信息
        String mapDataGet = "";
        mapDataGet += ZookeeperPath.FLOW_ZK_PATH_BASE.getKey() + SysFlow.ZK_GET_SEP;
        mapDataGet += String.valueOf(seqList.getZkProcess().getValue(FlowCfg.FLOW_YAML_CFG_CLUSTER.getKey()))
                + SysFlow.ZK_GET_SEP;
        // cache的key信息
        String nodeMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_NODE.getKey();

        // 创建schema路径并录入数据
        boolean nodeRsp = seqList.getZkProcess().createConfig(nodeMapKey, true, writeNodePath);

        LOGGER.info("flow to zk node path write rsp { nodeRsp :" + nodeRsp + "}");

        // 创建成功则进行流程，失败则删除节点
        if (nodeRsp) {
            return seqList.nextExec();
        }

        return seqList.rollExec();
    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {
        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 删除路由路径
        boolean ruleZkPathRsp = seqList.getZkProcess().deletePath(basePath, ZookeeperPath.FLOW_ZK_PATH_NODE.getKey());
        LOGGER.info("flow to rollback zk node path delete rsp:" + ruleZkPathRsp);

        return seqList.rollExec();
    }

}
