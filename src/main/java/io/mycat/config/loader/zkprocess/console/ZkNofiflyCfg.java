package io.mycat.config.loader.zkprocess.console;

/**
 * 进行zk通知的参数配制信息
* 源文件名：ZkNofiflyCfg.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public enum ZkNofiflyCfg {

    /**
     * 通知更新所有节点的信息
    * @字段说明 ZK_NOTIFLY_LOAD_ALL
    */
    ZK_NOTIFLY_LOAD_ALL("all"),
    
    ;

    /**
     * 配制的key的信息
    * @字段说明 key
    */
    private String key;

    private ZkNofiflyCfg(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
