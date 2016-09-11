package io.mycat.config.loader.zookeeper.create.flow;

import io.mycat.config.loader.zookeeper.create.comm.SeqLinkedList;
import io.mycat.config.loader.zookeeper.create.comm.ServiceExecInf;

/**
 * 将cache的配制信息录入到zk中
* 源文件名：FlowToCacheService.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class FlowToCacheService implements ServiceExecInf {

    @Override
    public boolean invoke(SeqLinkedList seqList) throws Exception {
        return false;
    }

    @Override
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception {
        return false;
    }

}
