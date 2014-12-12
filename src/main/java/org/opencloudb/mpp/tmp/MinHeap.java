package org.opencloudb.mpp.tmp;

import java.util.List;

import org.opencloudb.mpp.OrderCol;

public class MinHeap {

    private List<byte[]> data;
    private RowDataCmp cmp;
    private OrderCol orderCol;

    public MinHeap(List<byte[]> data, RowDataCmp cmp, OrderCol orderCol) {
        this.cmp = cmp;
        this.data = data;
        this.orderCol = orderCol;
        this.buildMinHeap();
    }

    private void buildMinHeap() {
        int len = data.size();
        for (int i = len / 2 - 1; i >= 0; i--) {
            heapify(i);
        }
    }

    private void heapify(int i) {
        int l = left(i);
        int r = right(i);
        int smallest = i;
        int len = data.size();
        if (l < len && cmp.compareObject(data.get(l), data.get(i), orderCol) < 0)
            smallest = l;
        if (r < len && cmp.compareObject(data.get(r), data.get(smallest), orderCol) < 0)
            smallest = r;
        if (i == smallest)
            return;
        swap(i, smallest);
        heapify(smallest);
    }

    private int right(int i) {
        return (i + 1) << 1;
    }

    private int left(int i) {
        return ((i + 1) << 1) - 1;
    }

    private void swap(int i, int j) {
        byte[] tmp = data.get(i);
        data.set(i, data.get(j));
        data.set(i, tmp);
    }

    public List<byte[]> getData() {
        return data;
    }

    public byte[] getRoot() {
        return data.get(0);
    }

    public void setRoot(byte[] root) {
        data.set(0, root);
        heapify(0);
    }

}
