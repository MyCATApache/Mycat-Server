package io.mycat.memory.unsafe.row;

import io.mycat.sqlengine.mpp.ColMeta;
import io.mycat.sqlengine.mpp.OrderCol;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * 结构信息
 *
 * Created by zagnix on 2016/6/6.
 */
public class StructType {

    /**
     * 列元信息集合
     */
    private final Map<String, ColMeta> columToIndx;
    /**
     * fields 数量
     */
    private final int fieldCount;
    /**
     * 排序列数组
     */
    private  OrderCol[] orderCols = null;

    public StructType(@Nonnull Map<String,ColMeta> columToIndx,int fieldCount){
        assert fieldCount >=0;
        this.columToIndx = columToIndx;
        this.fieldCount = fieldCount;
    }

    public int length() {
        return fieldCount;
    }

    public Map<String, ColMeta> getColumToIndx() {
        return columToIndx;
    }

    public OrderCol[] getOrderCols() {
        return orderCols;
    }

    public void setOrderCols(OrderCol[] orderCols) {
        this.orderCols = orderCols;
    }

    public long apply(int i) {
        return 0;
    }
}
