package io.mycat.config.loader.zookeeper.create.console;

/**
 * 流程配制信息
* 源文件名：FlowCfg.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public enum FlowCfg {

    /**
     * 配制的yaml文件的路径的信息
    * @字段说明 FLOW_CFG_YAML_FILE_MSG
    */
    FLOW_CFG_YAML_FILE_MSG("flow_cfg_yaml_file_info"),

    /**
     * yaml配制的集群的名称
    * @字段说明 FLOW_ZK_CFG_CLUSTER
    */
    FLOW_YAML_CFG_CLUSTER("cluster-name"),

    ;

    /**
     * 配制的key的信息
    * @字段说明 key
    */
    private String key;

    private FlowCfg(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
