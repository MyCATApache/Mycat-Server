package org.opencloudb.mpp.tmp;

import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.RowDataPacketSorter;
import org.opencloudb.net.mysql.RowDataPacket;

import java.util.*;

/**
 * 
 * @author coderczp-2014-12-8
 */
public class RowDataSorter extends RowDataPacketSorter {

    private int total;
    private HeapItf heap;
    private volatile int rtnSize;
    private volatile RowDataCmp cmp;
    private volatile boolean isDesc;
    private volatile boolean hasBuild;

    public RowDataSorter(OrderCol[] orderCols) {
        super(orderCols);
        this.cmp = new RowDataCmp(orderCols);
        if (orderCols.length > 0)
            this.isDesc = (orderCols[0].orderType == OrderCol.COL_ORDER_TYPE_DESC);
    }

    public synchronized void setLimt(int start, int size) {
        rtnSize = size;
        total = start + size;
        if (isDesc) {
            heap = new MinHeap(cmp, total);
        } else {
            heap = new MaxHeap(cmp, total);
        }
    }

    @Override
    public synchronized void addRow(RowDataPacket row) {
        if (heap.getData().size() < total) {
            heap.add(row);
            return;
        }
        if (heap.getData().size() == total && hasBuild == false) {
            hasBuild = true;
            heap.buildMinHeap();
        }
        heap.addIfRequired(row);
    }

    @Override
    public Collection<RowDataPacket> getSortedResult() {
        Vector<RowDataPacket> data = heap.getData();
        int size = data.size();
        if (size < 2)
            return data;
        if (size < 500)
            return sortAll(data);

        HeapItf rtnHeap;
//        if (isDesc) { // 降序取最小的几条数据
//            rtnHeap = new MaxHeap(cmp, rtnSize);
//        } else { // 升序取最大的几条数据
//            rtnHeap = new MinHeap(cmp, rtnSize);
//        }
        if (isDesc) { // 降序取最大的几条数据
            rtnHeap = new MinHeap(cmp, rtnSize);
        } else { // 升序取最小的几条数据
            rtnHeap = new MaxHeap(cmp, rtnSize);
        }
        int i = 0;
        while (i < rtnSize) {
            rtnHeap.add(data.get(i++));
        }
        rtnHeap.buildMinHeap();
        while (i < size) {
            rtnHeap.addIfRequired(data.get(i++));
        }
        Vector<RowDataPacket> dataRtn = rtnHeap.getData();
        Collections.sort(dataRtn, cmp);
        return dataRtn;

    }

    /**
     * 
     * 数据小的时候可以直接排序全部后取后几条数据
     * 
     * @param datas
     * @return
     */
    protected List<RowDataPacket> sortAll(Collection<RowDataPacket> datas) {
        int size = datas.size();
        RowDataPacket[] tmp = datas.toArray(new RowDataPacket[size]);
        Arrays.sort(tmp, cmp);
        LinkedList<RowDataPacket> rnt = new LinkedList<RowDataPacket>();
        int i = total - rtnSize;
        while (i < size)
            rnt.add(tmp[i++]);
        return rnt;
    }
}
