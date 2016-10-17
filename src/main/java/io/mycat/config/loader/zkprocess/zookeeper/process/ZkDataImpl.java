package io.mycat.config.loader.zkprocess.zookeeper.process;

import io.mycat.config.loader.zkprocess.zookeeper.DataInf;

/**
 * 数据节点信息
* 源文件名：DataImpl.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ZkDataImpl implements DataInf {

    /**
     * 名称信息
    * @字段说明 name
    */
    private String name;

    /**
     * 当前值信息
    * @字段说明 value
    */
    private String value;

    public ZkDataImpl(String name, String value) {
        super();
        this.name = name;
        this.value = value;
    }

    @Override
    public String getDataInfo() {
        return this.name + ":" + this.value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String getDataValue() {
        return this.value;
    }

}
