package io.mycat.config.loader.zkprocess.zookeeper.process;

import java.util.ArrayList;
import java.util.List;

import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;

/**
 * zk的目录节点信息
* 源文件名：ZkDirectoryMsg.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ZkDirectoryImpl implements DiretoryInf {

    /**
     * 整个节点信息
    * @字段说明 subordinateInfo
    */
    private List<Object> subordinateInfoList = new ArrayList<Object>();

    /**
     * 节点的名称信息
    * @字段说明 name
    */
    private String name;

    /**
     * 当前节点的数据信息
    * @字段说明 value
    */
    private String value;

    public ZkDirectoryImpl(String name, String value) {
        this.name = name;
        this.value = value;
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
    public String getDiretoryInfo() {
        return name + ":" + value;
    }

    @Override
    public void add(DiretoryInf branch) {
        this.subordinateInfoList.add(branch);
    }

    @Override
    public List<Object> getSubordinateInfo() {
        return this.subordinateInfoList;
    }

    @Override
    public void add(DataInf data) {
        this.subordinateInfoList.add(data);
    }

    @Override
    public String getDataName() {
        return this.name;
    }

}
