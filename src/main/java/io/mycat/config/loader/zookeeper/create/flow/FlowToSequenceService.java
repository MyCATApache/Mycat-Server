package io.mycat.config.loader.zookeeper.create.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;
import io.mycat.config.loader.zookeeper.create.console.FlowCfg;
import io.mycat.config.loader.zookeeper.create.console.SysFlow;

/**
 * 将序列信息存入到zk中
* 源文件名：FlowToSequenceService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FlowToSequenceService implements ServiceExecInf {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowToSequenceService.class);

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制sequence的配制信息
        String writeSequencePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey()
                + SysFlow.ZK_SEPARATOR;

        // map获取路径 信息
        String mapDataGet = "";
        mapDataGet += ZookeeperPath.FLOW_ZK_PATH_BASE.getKey() + SysFlow.ZK_GET_SEP;
        mapDataGet += String.valueOf(seqList.getZkProcess().getValue(FlowCfg.FLOW_YAML_CFG_CLUSTER.getKey()))
                + SysFlow.ZK_GET_SEP;
        // sequence的key信息
        String sequenceMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();

        // 创建schema路径并录入数据
        boolean sequenceRsp = seqList.getZkProcess().createConfig(sequenceMapKey, true, writeSequencePath);

        LOGGER.info("flow to zk sequence path write rsp { sequenceRsp:" + sequenceRsp + "}");

        // 创建成功则进行流程，失败则删除节点
        if (sequenceRsp) {
            return seqList.nextExec();
        }

        return seqList.rollExec();
    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 删除序列路径
        boolean sequenceZkPathRsp = seqList.getZkProcess().deletePath(basePath,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey());
        LOGGER.info("flow to rollback zk sequence path delete rsp:" + sequenceZkPathRsp);

        return seqList.rollExec();
    }

}
