package io.mycat.sqlengine.mpp.tmp;

import io.mycat.net.mysql.RowDataPacket;

import java.util.List;

/**
 * @author coderczp-2014-12-17
 */
public interface HeapItf {

    /**
     * 构建堆
     */
    void buildHeap();

    /**
     * 获取堆根节点
     *
     * @return
     */
    RowDataPacket getRoot();

    /**
     * 向堆添加元素
     *
     * @param row
     */
    void add(RowDataPacket row);

    /**
     * 获取堆数据
     *
     * @return
     */
    List<RowDataPacket> getData();

    /**
     * 设置根节点元素
     *
     * @param root
     */
    void setRoot(RowDataPacket root);

    /**
     * 向已满的堆添加元素
     *
     * @param row
     */
    boolean addIfRequired(RowDataPacket row);

    /**
     * 堆排序
     */
    void heapSort(int size);

}
