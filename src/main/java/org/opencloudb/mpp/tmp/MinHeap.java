package org.opencloudb.mpp.tmp;

import org.opencloudb.net.mysql.RowDataPacket;

public class MinHeap {

    private RowDataCmp cmp;
    private RowDataPacket[] data;

    public MinHeap(RowDataPacket[] data, RowDataCmp cmp) {
        this.cmp = cmp;
        this.data = data;
        this.buildMinHeap();
    }

    private void buildMinHeap() {
        int len = data.length;
        for (int i = len / 2 - 1; i >= 0; i--) {
            heapify(i);
        }
    }

    private void heapify(int i) {
        int l = left(i);
        int r = right(i);
        int smallest = i;
        int len = data.length;
        if (l < len && cmp.compare(data[l], data[i]) < 0)
            smallest = l;
        if (r < len && cmp.compare(data[r], data[smallest]) < 0)
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
        RowDataPacket tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }

    public RowDataPacket[] getData() {
        return data;
    }

    public RowDataPacket getRoot() {
        return data[0];
    }

    public void setRoot(RowDataPacket root) {
        data[0] = root;
        heapify(0);
    }

}
