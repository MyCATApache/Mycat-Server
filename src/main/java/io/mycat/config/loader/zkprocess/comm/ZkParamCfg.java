package io.mycat.config.loader.zkprocess.comm;

/**
 * 当前zk的配置参数信息
 * 源文件名：ZkParamCfg.java
 * 文件版本：1.0.0
 * 创建作者：liujun
 * 创建日期：2016年9月17日
 * 修改作者：liujun
 * 修改日期：2016年9月17日
 * 文件描述：TODO
 * 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
 */
public enum ZkParamCfg {

    /**
     * loadZk -- zk是否启用标识
     * @字段说明 ZK_CFG_OPEN
     */
    ZK_CFG_FLAG("loadZk"),

    /**
     * zkURL -- zk配置的url地址信息
     * @字段说明 ZK_CFG_URL
     */
    ZK_CFG_URL("zkURL"),

    /**
     * clusterId -- 集群的id
     * @字段说明 ZK_CFG_CLUSTERID
     */
    ZK_CFG_CLUSTERID("clusterId"),

    /**
     * clusterSize -- 集群的大小
     */
    ZK_CFG_CLUSTER_SIZE("clusterSize"),

    /**
     * myid -- 当前mycat节点的id
     * @字段说明 zk_CFG_MYID
     */
    ZK_CFG_MYID("myid"),

    /**
     * type --
     */
    MYCAT_SERVER_TYPE("type"),

    /**
     * boosterDataHosts --
     */
    MYCAT_BOOSTER_DATAHOSTS("boosterDataHosts"),

    /**
     * 集群中所有节点的名称信息
     * @字段说明 ZK_CFG_CLUSTER_NODES
     */
    ZK_CFG_CLUSTER_NODES("clusterNodes"),

    /**
     * 集群中所有节点的名称信息的分隔符
     * @字段说明 ZK_CFG_CLUSTER_NODES
     */
    ZK_CFG_CLUSTER_NODES_SEPARATE(","),

    ;

    private ZkParamCfg(String key) {
        this.key = key;
    }

    private String key;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
