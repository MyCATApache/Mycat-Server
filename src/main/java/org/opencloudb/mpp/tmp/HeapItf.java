package org.opencloudb.mpp.tmp;

import org.opencloudb.net.mysql.RowDataPacket;

import java.util.Vector;

/**
 * @author coderczp-2014-12-17
 */
public interface HeapItf {

    /**
     * 构建堆
     */
    public abstract void buildHeap();

    /**
     * 获取堆根节点
     *
     * @return
     */
    public abstract RowDataPacket getRoot();

    /**
     * 向堆添加元素
     *
     * @param row
     */
    public abstract void add(RowDataPacket row);

    /**
     * 获取堆数据
     *
     * @return
     */
    public abstract Vector<RowDataPacket> getData();

    /**
     * 设置根节点元素
     *
     * @param root
     */
    public abstract void setRoot(RowDataPacket root);

    /**
     * 向已满的堆添加元素
     *
     * @param row
     */
    public abstract void addIfRequired(RowDataPacket row);

    /**
     * 堆排序
     */
    public abstract void heapSort();

}
