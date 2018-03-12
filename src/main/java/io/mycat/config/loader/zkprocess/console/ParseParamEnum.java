package io.mycat.config.loader.zkprocess.console;

/**
 * 转换的流程参数配制信息
* 源文件名：ParseParamEnum.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月18日
* 修改作者：liujun
* 修改日期：2016年9月18日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public enum ParseParamEnum {

    /**
     * mapfile配制的参数名
    * @字段说明 ZK_PATH_RULE_MAPFILE_NAME
    */
    ZK_PATH_RULE_MAPFILE_NAME("mapFile"),

    ;

    /**
     * 配制的key的信息
    * @字段说明 key
    */
    private String key;

    private ParseParamEnum(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
