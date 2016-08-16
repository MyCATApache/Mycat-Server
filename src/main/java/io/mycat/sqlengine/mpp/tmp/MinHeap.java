package io.mycat.sqlengine.mpp.tmp;

import java.util.ArrayList;
import java.util.List;

import io.mycat.net.mysql.RowDataPacket;

/**
 * 最小堆排序，适用于倒序排序
 *
 * @author coderczp-2014-12-8
 */
public class MinHeap implements HeapItf {

    private RowDataCmp cmp;
    private List<RowDataPacket> data;

    public MinHeap(RowDataCmp cmp, int size) {
        this.cmp = cmp;
        this.data = new ArrayList<>();
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
        if (l < size && cmp.compare(data.get(l), data.get(i)) < 0) {
            smallest = l;
        }
        if (r < size && cmp.compare(data.get(r), data.get(smallest)) < 0) {
            smallest = r;
        }
        if (i == smallest) {
            return;
        }
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
        RowDataPacket tmp = data.get(i);
        RowDataPacket elementAt = data.get(j);
        data.set(i, elementAt);
        data.set(j, tmp);
    }

    public RowDataPacket getRoot() {
        return data.get(0);
    }

    public void setRoot(RowDataPacket root) {
        data.set(0, root);
        heapify(0, data.size());
    }

    public List<RowDataPacket> getData() {
        return data;
    }

    public void add(RowDataPacket row) {
        data.add(row);
    }

    @Override
    public boolean addIfRequired(RowDataPacket row) {
        // 淘汰堆里最小的数据
        RowDataPacket root = getRoot();
        if (cmp.compare(row, root) > 0) {
            setRoot(row);
            return true;
        }
        return false;
    }

    @Override
    public void heapSort(int size) {
        final int total = data.size();
        //容错处理
        if (size <= 0 || size > total) {
            size = total;
        }
        final int min = size == total ? 0 : (total - size - 1);

        //末尾与头交换，交换后调整最大堆
        for (int i = total - 1; i > min; i--) {
            swap(0, i);
            heapify(0, i);
        }
    }

}
