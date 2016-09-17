package io.mycat.config.loader.zkprocess.zookeeper;

/**
 * 数据节点信息
* 源文件名：DataInf.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public interface DataInf {

    /**
     * 获取信息,以:分隔两个值 
     * @return
     */
    String getDataInfo();

    /**
     * 返回数据节点值信息
    * 方法描述
    * @return
    * @创建日期 2016年9月17日
    */
    String getDataValue();

}
