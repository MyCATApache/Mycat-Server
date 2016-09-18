package io.mycat.config.loader.zookeeper.create.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;
import io.mycat.config.loader.zookeeper.create.console.FlowCfg;
import io.mycat.config.loader.zookeeper.create.console.SysFlow;

/**
 * 进行服务信息的录入
* 源文件名：FlowToServerService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FlowToServerService implements ServiceExecInf {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowToServerService.class);

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制基本的server路径
        String writeServerBasePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FLOW_ZK_PATH_SERVER.getKey()
                + SysFlow.ZK_SEPARATOR;
        // 执行default
        String defabultCfg = writeServerBasePath + ZookeeperPath.FLOW_ZK_PATH_SERVER_DEFAULT.getKey();
        // 集群配制信息
        String clusterCfg = writeServerBasePath + ZookeeperPath.FLOW_ZK_PATH_SERVER_CLUSTER.getKey();
        // 用户信息
        String userCfg = writeServerBasePath + ZookeeperPath.FLOW_ZK_PATH_SERVER_USER.getKey();
        // 黑白名单信息
        String fireWallCfg = writeServerBasePath + ZookeeperPath.FLOW_ZK_PATH_SERVER_FIREWALL.getKey();
        // 授权信息
        String authCfg = writeServerBasePath + ZookeeperPath.FLOW_ZK_PATH_SERVER_AUTH.getKey();

        // map获取路径 信息
        String mapDataGet = "";
        mapDataGet += ZookeeperPath.FLOW_ZK_PATH_BASE.getKey() + SysFlow.ZK_GET_SEP;
        mapDataGet += String.valueOf(seqList.getZkProcess().getValue(FlowCfg.FLOW_YAML_CFG_CLUSTER.getKey()))
                + SysFlow.ZK_GET_SEP + ZookeeperPath.FLOW_ZK_PATH_SERVER.getKey() + SysFlow.ZK_GET_SEP;
        // 路由的key信息
        String defaultMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SERVER_DEFAULT.getKey();
        // 集群
        String clusterMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SERVER_CLUSTER.getKey();
        // 用户
        String userMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SERVER_USER.getKey();
        // 黑白名单信息
        String fireWallMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SERVER_FIREWALL.getKey();
        // 授权信息
        String authWallMapKey = mapDataGet + ZookeeperPath.FLOW_ZK_PATH_SERVER_AUTH.getKey();

        // 创建default路径并录入数据
        boolean defaultRsp = seqList.getZkProcess().createConfig(defaultMapKey, true, defabultCfg);
        // 配制cluster集群
        boolean clusterRsp = seqList.getZkProcess().createConfig(clusterMapKey, true, clusterCfg);
        // 用户配制
        boolean userRsp = seqList.getZkProcess().createConfig(userMapKey, true, userCfg);
        // 黑白名单
        boolean fireWallRsp = seqList.getZkProcess().createConfig(fireWallMapKey, true, fireWallCfg);
        // 权限信息
        boolean authRsp = seqList.getZkProcess().createConfig(authWallMapKey, true, authCfg);

        LOGGER.info("flow to zk server path write rsp { defaultRsp:" + defaultRsp + "}");
        LOGGER.info("flow to zk server path write rsp { clusterRsp:" + clusterRsp + "}");
        LOGGER.info("flow to zk server path write rsp { userRsp:" + userRsp + "}");
        LOGGER.info("flow to zk server path write rsp { fireWallRsp:" + fireWallRsp + "}");
        LOGGER.info("flow to zk server path write rsp { authRsp:" + authRsp + "}");

        // 创建成功则进行流程，失败则删除节点
        if (defaultRsp && clusterRsp && userRsp && fireWallRsp && authRsp) {
            return seqList.nextExec();
        }

        return seqList.rollExec();

    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {

        // 获得集群名称:
        String basePath = seqList.getZkProcess().getBasePath();

        // 配制基本的server路径
        basePath = basePath + SysFlow.ZK_SEPARATOR + ZookeeperPath.FLOW_ZK_PATH_SERVER.getKey() + SysFlow.ZK_SEPARATOR;
        // 执行default
        boolean defaultRsp = seqList.getZkProcess().deletePath(basePath,
                ZookeeperPath.FLOW_ZK_PATH_SERVER_DEFAULT.getKey());
        // 集群配制信息
        boolean clusterRsp = seqList.getZkProcess().deletePath(basePath,
                ZookeeperPath.FLOW_ZK_PATH_SERVER_CLUSTER.getKey());
        // 用户信息
        boolean userRsp = seqList.getZkProcess().deletePath(basePath, ZookeeperPath.FLOW_ZK_PATH_SERVER_USER.getKey());
        // 黑白名单信息
        boolean fireWallRsp = seqList.getZkProcess().deletePath(basePath,
                ZookeeperPath.FLOW_ZK_PATH_SERVER_FIREWALL.getKey());
        // 授权信息
        boolean authRsp = seqList.getZkProcess().deletePath(basePath, ZookeeperPath.FLOW_ZK_PATH_SERVER_AUTH.getKey());

        LOGGER.info("flow to rollback zk server path write rsp { defaultRsp:" + defaultRsp + "}");
        LOGGER.info("flow to rollback zk server path write rsp { clusterRsp:" + clusterRsp + "}");
        LOGGER.info("flow to rollback zk server path write rsp { userRsp:" + userRsp + "}");
        LOGGER.info("flow to rollback zk server path write rsp { fireWallRsp:" + fireWallRsp + "}");
        LOGGER.info("flow to rollback zk server path write rsp { authRsp:" + authRsp + "}");

        basePath = basePath.substring(0, basePath.length() - 1);
        // 删除server目录
        boolean serverZkRsp = seqList.getZkProcess().deletePath(basePath);
        LOGGER.info("flow to rollback zk server path write rsp { serverZkRsp:" + serverZkRsp + "}");

        return seqList.rollExec();
    }

}
