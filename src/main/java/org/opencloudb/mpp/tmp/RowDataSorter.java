package org.opencloudb.mpp.tmp;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.RowDataPacketSorter;
import org.opencloudb.net.mysql.RowDataPacket;

/**
 * 
 * @author coderczp-2014-12-8
 */
public class RowDataSorter extends RowDataPacketSorter {

    private MinHeap heap;
    private AtomicInteger add;
    private int total;
    private int returnSize;
    private volatile int addCount;
    private final RowDataCmp cmp;

    public RowDataSorter(OrderCol[] orderCols) {
        super(orderCols);
        this.cmp = new RowDataCmp(orderCols);
        this.add = new AtomicInteger();
    }

    public synchronized void setLimt(int start, int size) {
        returnSize = size;
        total = start + size;
        heap = new MinHeap(cmp, total);
    }

    @Override
    public synchronized void addRow(RowDataPacket row) {
        if (add.getAndIncrement() < total) {
            addCount++;
            heap.add(row);
            return;
        }
        if (add.get() == total) {
            heap.buildMinHeap();
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
            if (addCount == 0 || returnSize == 0)
                return lst;
            int rtnSize = Math.min(returnSize, addCount);
            Vector<RowDataPacket> data = heap.getData();
            MinHeap rtnHeap = new MinHeap(cmp, rtnSize);
            for (int i = 0; i < rtnSize; i++) {
                rtnHeap.add(data.get(i));
            }
            for (int i = rtnSize; i < addCount; i++) {
                RowDataPacket root = data.get(i);
                if (root == null) {
                    continue;
                } else if (cmp.compare(root, rtnHeap.getRoot()) > 0) {
                    rtnHeap.setRoot(root);
                } else {
                    data.set(i, null);
                }
            }
            Collections.sort(rtnHeap.getData(), cmp);
            return rtnHeap.getData();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lst;
    }

}
