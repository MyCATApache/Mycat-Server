package io.mycat.config.loader.zookeeper.create.flow;

import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;
import io.mycat.config.loader.zookeeper.create.console.FlowCfg;

/**
 * 生成在线信息目录
* 源文件名：FlowToLineService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FlowToLineService implements ServiceExecInf {

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 执行创建路径操作
        boolean crRsp = seqList.getZkProcess().createPath(basePath, FlowCfg.FLOW_ZK_PATH_LINE.getKey());

        // 创建成功则进行流程，失败则删除节点
        if (crRsp) {
            return seqList.nextExec();
        }
        return seqList.rollExec();
    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 执行删除路径操作
        seqList.getZkProcess().deletePath(basePath, FlowCfg.FLOW_ZK_PATH_LINE.getKey());

        return seqList.rollExec();
    }

}
