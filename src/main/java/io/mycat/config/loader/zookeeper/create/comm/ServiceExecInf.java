package io.mycat.config.loader.zookeeper.create.comm;

/**
 * 责任链业务实现 接口
* 源文件名：ServiceExecInf.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年6月19日
* 修改作者：liujun
* 修改日期：2016年6月19日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public interface ServiceExecInf {

    /**
     * 进行正常流程执行的代码
     * 
     * @param seq
     * @return
     * @throws Exception
     */
    public boolean invoke(SeqLinkedList seqList) throws Exception;

    /**
     * 进行回退流程操作
     * 
     * @param seqlist
     * @return
     * @throws Exception
     */
    public boolean rollBackInvoke(SeqLinkedList seqList) throws Exception;

}
