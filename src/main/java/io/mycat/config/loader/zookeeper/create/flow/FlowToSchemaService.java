package io.mycat.config.loader.zookeeper.create.flow;

import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;
import io.mycat.config.loader.zookeeper.create.comm.ZkProcessBase;
import io.mycat.config.loader.zookeeper.create.console.FlowCfg;
import io.mycat.config.loader.zookeeper.create.console.SysFlow;

/**
 * 将schema的配制信息录入到zk中
* 源文件名：FlowToSchemaService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FlowToSchemaService extends ZkProcessBase implements ServiceExecInf {

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        String writePath = basePath + SysFlow.ZK_SEPARATOR + FlowCfg.FOW_ZK_PATH_SCHEMA.getKey();
        writePath += SysFlow.ZK_SEPARATOR + FlowCfg.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey();

        String getPaths = "";

        getPaths += FlowCfg.FLOW_ZK_PATH_BASE.getKey() + SysFlow.ZK_GET_SEP;
        getPaths += String.valueOf(seqList.getZkProcess().getZkConfig().get(FlowCfg.FLOW_YAML_CFG_CLUSTER.getKey()))
                + SysFlow.ZK_GET_SEP;
        getPaths += FlowCfg.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey();

        // 执行创建路径并录入数据
        boolean crRsp = seqList.getZkProcess().createConfig(getPaths, true, writePath);
        // 创建成功则进行流程，失败则删除节点
        if (crRsp) {
            return seqList.nextExec();
        }

        return false;
    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {
        return false;
    }

}
