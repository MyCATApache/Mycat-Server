package org.opencloudb.mpp.tmp;

import org.opencloudb.net.mysql.RowDataPacket;

import java.util.Vector;

/**
 * 最小堆排序，适用于倒序排序
 *
 * @author coderczp-2014-12-8
 */
public class MinHeap implements HeapItf {

    private RowDataCmp cmp;
    private Vector<RowDataPacket> data;

    public MinHeap(RowDataCmp cmp, int size) {
        this.cmp = cmp;
        this.data = new Vector<RowDataPacket>();
    }

    @Override
    public void buildHeap() {
        int len = data.size();
        for (int i = len / 2 - 1; i >= 0; i--) {
            heapify(i, len);
        }
    }

    private void heapify(int i, int size) {
        int l = left(i);
        int r = right(i);
        int smallest = i;
        if (l < size && cmp.compare(data.elementAt(l), data.elementAt(i)) < 0)
            smallest = l;
        if (r < size && cmp.compare(data.elementAt(r), data.elementAt(smallest)) < 0)
            smallest = r;
        if (i == smallest)
            return;
        swap(i, smallest);
        heapify(smallest, size);
    }

    private int right(int i) {
        return (i + 1) << 1;
    }

    private int left(int i) {
        return ((i + 1) << 1) - 1;
    }

    private void swap(int i, int j) {
        RowDataPacket tmp = data.elementAt(i);
        RowDataPacket elementAt = data.elementAt(j);
        data.set(i, elementAt);
        data.set(j, tmp);
    }

    public RowDataPacket getRoot() {
        return data.elementAt(0);
    }

    public void setRoot(RowDataPacket root) {
        data.set(0, root);
        heapify(0, data.size());
    }

    public Vector<RowDataPacket> getData() {
        return data;
    }

    public void add(RowDataPacket row) {
        data.add(row);
    }

    @Override
    public void addIfRequired(RowDataPacket row) {
        // 淘汰堆里最小的数据
        RowDataPacket root = getRoot();
        if (cmp.compare(row, root) > 0) {
            setRoot(row);
        }
    }

    @Override
    public void heapSort() {
        //末尾与头交换，交换后调整最小堆
        for (int i = data.size() - 1; i > 0; i--) {
            swap(0, i);
            heapify(0, i);
        }
    }

}
