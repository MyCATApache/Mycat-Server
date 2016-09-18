package io.mycat.config.loader.zookeeper.create.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;
import io.mycat.config.loader.zookeeper.create.console.SysFlow;

/**
 * 将切换信息，以及迁移的信息录入zk
* 源文件名：FlowToBinDataService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FlowToBinDataService implements ServiceExecInf {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowToBinDataService.class);

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制bindata的配制信息
        String writePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FLOW_ZK_PATH_BINDATA.getKey()
                + SysFlow.ZK_SEPARATOR;

        // 创建dnindex路径
        boolean dnindexRsp = seqList.getZkProcess().createPath(writePath,
                ZookeeperPath.FLOW_ZK_PATH_BINDATA_DNINDEX.getKey());
        // 创建datanode路径
        boolean moveRsp = seqList.getZkProcess().createPath(writePath,
                ZookeeperPath.FLOW_ZK_PATH_BINDATA_MOVE.getKey());

        LOGGER.info("flow to zk bindata path write rsp { dnindexRsp:" + dnindexRsp + ",moveRsp :" + moveRsp + "}");

        // 创建成功则进行流程，失败则删除节点
        if (dnindexRsp && moveRsp) {
            return seqList.nextExec();
        }

        return seqList.rollExec();
    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制bindata的配制信息
        basePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FLOW_ZK_PATH_BINDATA.getKey() + SysFlow.ZK_SEPARATOR;
        // 状态切换后的信息
        boolean dnindexZkPath = seqList.getZkProcess().deletePath(basePath,
                ZookeeperPath.FLOW_ZK_PATH_BINDATA_DNINDEX.getKey());
        // 迁移路由信息
        boolean moveZkPath = seqList.getZkProcess().deletePath(basePath,
                ZookeeperPath.FLOW_ZK_PATH_BINDATA_MOVE.getKey());
        // 删除路由路径
        LOGGER.info("flow to rollback zk bindata  path dnindex delete rsp:" + dnindexZkPath);
        LOGGER.info("flow to rollback zk bindata  path move delete rsp:" + moveZkPath);

        // 删除bindata路径
        // 去掉尾符号
        basePath = basePath.substring(0, basePath.length() - 1);

        // 状态切换后的信息
        boolean bindataZkPath = seqList.getZkProcess().deletePath(basePath);
        LOGGER.info("flow to rollback zk bindata  path  delete rsp:" + bindataZkPath);

        return seqList.rollExec();

    }

}
