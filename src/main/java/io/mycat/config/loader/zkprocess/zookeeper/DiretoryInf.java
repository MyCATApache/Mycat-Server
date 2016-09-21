package io.mycat.config.loader.zkprocess.zookeeper;

import java.util.List;

/**
 * 目录接口信息
* 源文件名：DiretoryInf.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public interface DiretoryInf {

    /**
     * 获取当前的目录信息
     * @return
     */
    String getDiretoryInfo();

    /**
     * 添加目录或者数据节点
     * @param branch
     */
    void add(DiretoryInf directory);

    /**
     * 添加数据节点信息
    * 方法描述
    * @param data
    * @创建日期 2016年9月15日
    */
    void add(DataInf data);

    /**
     * 获取子节点信息
     * @return
     */
    List<Object> getSubordinateInfo();

    /**
     * 获取节点的名称
    * @字段说明 getDataName
    */
    String getDataName();

}
