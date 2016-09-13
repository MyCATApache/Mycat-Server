package io.mycat.config.loader.zookeeper.create.comm;

import java.util.LinkedList;
import java.util.List;

/**
 * 进行核心的责伤链流程存放
* 源文件名：SeqLinkedList.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年6月19日
* 修改作者：liujun
* 修改日期：2016年6月19日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SeqLinkedList {

    /**
     * 用来存放流程的容器
     */
    private List<ServiceExecInf> linkedServ = new LinkedList<ServiceExecInf>();

    /**
     * 定义回退流程操作
     */
    private LinkedList<ServiceExecInf> rollBackList = new LinkedList<ServiceExecInf>();

    /**
     * zk公共操作信息
    * @字段说明 zkProcess
    */
    private ZkProcessBase zkProcess = new ZkProcessBase();

    /**
     * 添加流程代码
     * 
     * @param serviceExec
     */
    public void addExec(ServiceExecInf serviceExec) {
        this.linkedServ.add(serviceExec);
    }

    /**
     * 添加正常执行流程
     * 
     * @param serviceExec
     *            [] 流程执行数组
     */
    public void addExec(ServiceExecInf[] serviceExec) {
        if (null != serviceExec) {
            for (int i = 0; i < serviceExec.length; i++) {
                this.linkedServ.add(serviceExec[i]);
            }
        }
    }

    /**
     * 添加回退流程代码
     * 
     * @param serviceExec
     */
    public void addRollbackExec(ServiceExecInf serviceExec) {
        this.linkedServ.add(serviceExec);
    }

    /**
     * 添加回退执行流程
     * 
     * @param serviceExec
     *            [] 流程执行数组
     */
    public void addRollbackExec(ServiceExecInf[] serviceExec) {
        if (null != serviceExec) {
            for (int i = 0; i < serviceExec.length; i++) {
                this.rollBackList.add(serviceExec[i]);
            }
        }
    }

    /**
     * 执行下一个流程代码
     * 
     * @return
     * @throws Exception
     */
    public boolean nextExec() throws Exception {

        if (null != linkedServ && linkedServ.size() > 0) {

            ServiceExecInf servExec = linkedServ.remove(0);
            
            rollBackList.addFirst(servExec);

            return servExec.invoke(this);
        } else {
            return true;
        }
    }

    /**
     * 进行回退代码操作
     * 
     * @return
     * @throws Exception
     */
    public boolean rollExec() throws Exception {
        if (null != rollBackList && rollBackList.size() > 0) {
            //倒序执行
            ServiceExecInf rollExec = rollBackList.removeFirst();

            return rollExec.rollBackInvoke(this);
        } else {
            return false;
        }
    }

    public ZkProcessBase getZkProcess() {
        return zkProcess;
    }

    public void setZkProcess(ZkProcessBase zkProcess) {
        this.zkProcess = zkProcess;
    }

}
