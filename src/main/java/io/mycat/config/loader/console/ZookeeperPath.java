package io.mycat.config.loader.console;

/**
 * 专门用来操作zookeeper路径的文件信息
* 源文件名：ZkPath.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public enum ZookeeperPath {

    /**
     * zk的路径分隔符
    * @字段说明 ZK_SEPARATOR
    */
    ZK_SEPARATOR("/"),

    /**
     * 最基础的mycat节点
     * @字段说明 FLOW_ZK_PATH_LINE
     */
    FLOW_ZK_PATH_BASE("mycat"),

    /**
     * 在当前在线的节点
    * @字段说明 FLOW_ZK_PATH_LINE
    */
    FLOW_ZK_PATH_LINE("line"),

    /**
     * schema父路径
    * @字段说明 FOW_ZK_PATH_SCHEMA
    */
    FOW_ZK_PATH_SCHEMA("schema"),

    /**
     * 配制schema信息
     * @字段说明 FLOW_ZK_PATH_SCHEMA
     */
    FLOW_ZK_PATH_SCHEMA_SCHEMA("schema"),

    /**
     * 对应数据库信息
    * @字段说明 FLOW_ZK_PATH_SCHEMA_DATANODE
    */
    FLOW_ZK_PATH_SCHEMA_DATANODE("dataNode"),

    /**
     * 数据库信息dataHost
     * @字段说明 FLOW_ZK_PATH_SCHEMA_DATANODE
     */
    FLOW_ZK_PATH_SCHEMA_DATAHOST("dataHost"),

    /**
     * 路由信息
     * @字段说明 FLOW_ZK_PATH_SCHEMA_DATANODE
     */
    FLOW_ZK_PATH_RULE("rules"),

    /**
     * 路由信息
     * @字段说明 FLOW_ZK_PATH_SCHEMA_DATANODE
     */
    FLOW_ZK_PATH_RULE_TABLERULE("tableRule"),

    /**
     * 路由信息
     * @字段说明 FLOW_ZK_PATH_SCHEMA_DATANODE
     */
    FLOW_ZK_PATH_RULE_FUNCTION("function"),

    /**
     * 服务端配制路径
    * @字段说明 FLOW_ZK_PATH_SERVER
    */
    FLOW_ZK_PATH_SERVER("server"),

    /**
     * 默认配制信息
    * @字段说明 FLOW_ZK_PATH_SERVER_DEFAULT
    */
    FLOW_ZK_PATH_SERVER_DEFAULT("default"),

    /**
     * 针对集群的配制信息
     * @字段说明 FLOW_ZK_PATH_SERVER_DEFAULT
     */
    FLOW_ZK_PATH_SERVER_CLUSTER("cluster"),

    /**
     * 配制的用户信息
     * @字段说明 FLOW_ZK_PATH_SERVER_DEFAULT
     */
    FLOW_ZK_PATH_SERVER_USER("user"),

    /**
     * 配制的防火墙信息,如黑白名单信息
     * @字段说明 FLOW_ZK_PATH_SERVER_DEFAULT
     */
    FLOW_ZK_PATH_SERVER_FIREWALL("firewall"),

    /**
     * 表的权限信息
    * @字段说明 FLOW_ZK_PATH_SERVER_AUTH
    */
    FLOW_ZK_PATH_SERVER_AUTH("auth"),

    /**
     * 序列信息
     * @字段说明 FLOW_ZK_PATH_SERVER_AUTH
     */
    FLOW_ZK_PATH_SEQUENCE("sequences"),

    /**
     * 序列信息中公共配制信息
     * @字段说明 FLOW_ZK_PATH_SERVER_AUTH
     */
    FLOW_ZK_PATH_SEQUENCE_COMMON("common"),

    /**
     * 用来存放序列值的信息
     * @字段说明 FLOW_ZK_PATH_SERVER_AUTH
     */
    FLOW_ZK_PATH_SEQUENCE_INSTANCE("instance"),

    /**
     * 用来存放序列值的
     * @字段说明 FLOW_ZK_PATH_SERVER_AUTH
     */
    FLOW_ZK_PATH_SEQUENCE_LEADER("leader"),
    
    /**
     * 递增序列号
     * @字段说明 FLOW_ZK_PATH_SERVER_AUTH
     */
    FLOW_ZK_PATH_SEQUENCE_INCREMENT_SEQ("incr_sequence"),

    /**
     * 序列信息中需要单独节点配制的信息
     * @字段说明 FLOW_ZK_PATH_SERVER_AUTH
     */
    FLOW_ZK_PATH_SEQUENCE_CLUSTER("cluster"),

    /**
     * 缓存信息
    * @字段说明 FLOW_ZK_PATH_CACHE
    */
    FLOW_ZK_PATH_CACHE("cache"),

    /**
     * 配制切换及状态目录信息
    * @字段说明 FLOW_ZK_PATH_BINDATA
    */
    FLOW_ZK_PATH_BINDATA("bindata"),


    /**
     * 配制切换及状态目录信息
     * @字段说明 FLOW_ZK_PATH_RULEDATA
     */
    FLOW_ZK_PATH_RULEATA("ruledata"),
    /**
     * dnindex切换信息
     * @字段说明 FLOW_ZK_PATH_CACHE
     */
    FLOW_ZK_PATH_BINDATA_DNINDEX("dnindex"),

    /**
     * 迁移的信息
     * @字段说明 FLOW_ZK_PATH_CACHE
     */
    FLOW_ZK_PATH_BINDATA_MOVE("move"),

    /**
     * 节点单独的配制信息
    * @字段说明 FLOW_ZK_PATH_NODE
    */
    FLOW_ZK_PATH_NODE("node"),

    /**zk写入本地的路径信息
    * @字段说明 ZK_LOCAL_WRITE_PATH
    */
    ZK_LOCAL_WRITE_PATH("./"),
    // /**zk写入本地的路径信息
    // * @字段说明 ZK_LOCAL_WRITE_PATH
    // */
    // ZK_LOCAL_WRITE_PATH("zkdownload/"),

    /**
     * zk本地配制目录信息
     * @字段说明 ZK_LOCAL_WRITE_PATH
     */
    ZK_LOCAL_CFG_PATH("/zkconf/"),

    ;
    /**
     * 配制的key的信息
    * @字段说明 key
    */
    private String key;

    private ZookeeperPath(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
