package io.mycat.config.loader.zookeeper.create.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
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

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowToSchemaService.class);

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制schema的配制信息
        String writePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FOW_ZK_PATH_SCHEMA.getKey()
                + SysFlow.ZK_SEPARATOR;
        String schemaZkPath = writePath + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey();
        String dataNodeZkPath = writePath + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATANODE.getKey();
        String dataHostZkPath = writePath + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATAHOST.getKey();

        // map获取路径 信息
        String mapDataGet = "";
        mapDataGet += ZookeeperPath.FLOW_ZK_PATH_BASE.getKey() + SysFlow.ZK_GET_SEP;
        mapDataGet += String.valueOf(seqList.getZkProcess().getValue(FlowCfg.FLOW_YAML_CFG_CLUSTER.getKey()))
                + SysFlow.ZK_GET_SEP + ZookeeperPath.FOW_ZK_PATH_SCHEMA.getKey() + SysFlow.ZK_GET_SEP;

        String schemaMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey();
        String dataNodeMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATANODE.getKey();
        String dataHostMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATAHOST.getKey();

        // 创建schema路径并录入数据
        boolean schemaRsp = seqList.getZkProcess().createConfig(schemaMapKey, true, schemaZkPath);
        // 创建datanode路径并写入数据
        boolean dataNodeRsp = seqList.getZkProcess().createConfig(dataNodeMapKey, true, dataNodeZkPath);
        // 写入datahost
        boolean dataHostRsp = seqList.getZkProcess().createConfig(dataHostMapKey, true, dataHostZkPath);

        LOGGER.info("flow to zk scheam path write rsp { schemaRsp:" + schemaRsp + ",dataNodeRsp :" + dataNodeRsp
                + ",dataHostRsp:" + dataHostRsp + "}");

        // 创建成功则进行流程，失败则删除节点
        if (schemaRsp && dataNodeRsp && dataHostRsp) {
            return seqList.nextExec();
        }

        return seqList.rollExec();
    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制schema的配制信息
        String writePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FOW_ZK_PATH_SCHEMA.getKey()
                + SysFlow.ZK_SEPARATOR;

        // 删除schema路径
        boolean schemaZkPathRsp = seqList.getZkProcess().deletePath(writePath,
                ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey());
        LOGGER.info("flow to rollback zk schema schema path delete rsp:" + schemaZkPathRsp);
        // 删除dataNodeZkPath节点
        boolean dataNodeZkPathRsp = seqList.getZkProcess().deletePath(writePath,
                ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATANODE.getKey());
        LOGGER.info("flow to rollback zk schema dataNode path delete rsp:" + dataNodeZkPathRsp);
        // 删除dataNodeZkPath节点
        boolean dataHostZkPathRsp = seqList.getZkProcess().deletePath(writePath,
                ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATAHOST.getKey());
        LOGGER.info("flow to rollback zk schema dataHost path delete rsp:" + dataHostZkPathRsp);

        writePath = writePath.substring(0, writePath.length() - 1);

        // 删除schema目录
        boolean schemaZkRsp = seqList.getZkProcess().deletePath(writePath);
        LOGGER.info("flow to rollback zk schema path write rsp { schemaZkRsp:" + schemaZkRsp + "}");

        return seqList.rollExec();
    }

}
