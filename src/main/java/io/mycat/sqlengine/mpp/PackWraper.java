package io.mycat.sqlengine.mpp;


/**
 * Created by zagnix on 2016/7/6.
 */

/**
 * 一行数据是从哪个节点来的。
 * 通过dataNode查找对应的sorter，
 * 将数据放到对应的datanode的sorter，
 * 进行排序.
 */
public final  class PackWraper {
    public byte[] rowData;
    public String dataNode;
}
