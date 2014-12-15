package org.opencloudb.mpp.tmp;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.RowDataPacketSorter;
import org.opencloudb.net.mysql.RowDataPacket;

public class RowDataSorter extends RowDataPacketSorter {

    private MinHeap heap;
    private AtomicInteger add;
    private int total;
    private int returnSize;
    private RowDataPacket[] topN;
    private volatile int addCount;
    private volatile RowDataCmp cmp;

    public RowDataSorter(OrderCol[] orderCols) {
        super(orderCols);
        this.cmp = new RowDataCmp(orderCols);
        this.add = new AtomicInteger();
    }

    public synchronized void setLimt(int start, int size) {
        returnSize = size;
        total = start + size;
        topN = new RowDataPacket[total];
    }

    @Override
    public synchronized void addRow(RowDataPacket row) {
        if (add.get() < total) {
            topN[add.getAndIncrement()] = row;
            addCount++;
            return;
        }
        if (add.get() == total) {
            heap = new MinHeap(topN, cmp);
            add.getAndIncrement();
            return;
        }
        RowDataPacket root = heap.getRoot();
        if (cmp.compare(row, root) > 0) {
            heap.setRoot(row);
        }
    }

    @Override
    public Collection<RowDataPacket> getSortedResult() {
        LinkedList<RowDataPacket> lst = new LinkedList<RowDataPacket>();
        try {
            if (addCount == 0)
                return lst;
            int rtnSize = Math.min(returnSize, addCount);
            RowDataPacket[] rtn = new RowDataPacket[rtnSize];
            System.arraycopy(topN, 0, rtn, 0, rtnSize);
            MinHeap rtnHeap = new MinHeap(rtn, cmp);
            for (int i = rtnSize; i < addCount; i++) {
                RowDataPacket root = topN[i];
                if (root == null) {
                    continue;
                } else if (cmp.compare(root, rtnHeap.getRoot()) > 0) {
                    rtnHeap.setRoot(root);
                } else {
                    topN[i] = null;
                }
            }
            Arrays.sort(rtn, cmp);
            Collections.addAll(lst, rtn);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lst;
    }

}
