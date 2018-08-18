package io.mycat.sqlengine.mpp.tmp;

import java.util.*;

/**
 * 
 * @author coderczp-2014-12-8
 */
public class IntMinHeap {

    private int i;
    private int[] data;

    public IntMinHeap(int[] data) {
        this.data = data;
    }

    public void buildMinHeap() {
        int len = data.length;
        for (int i = len / 2 - 1; i >= 0; i--) {
            heapify(i);
        }
    }

    private void heapify(int i) {
        int l = left(i);
        int r = right(i);
        int max = i;
        int len = data.length;
        if (l < len && data[l] < data[i]) {
            max = l;
        }
        if (r < len && data[r] < data[max]) {
            max = r;
        }
        if (i == max) {
            return;
        }
        swap(i, max);
        heapify(max);
    }

    private int right(int i) {
        return (i + 1) << 1;
    }

    private int left(int i) {
        return ((i + 1) << 1) - 1;
    }

    private void swap(int i, int j) {
        int tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }

    public int getRoot() {
        return data[0];
    }

    public void setRoot(int root) {
        data[0] = root;
        heapify(0);
    }

    public int[] getData() {
        return data;
    }

    public synchronized void add(int row) {
        data[i++] = row;
    }

    // 淘汰堆里最大的数据
    public void addIfRequired(int row) {
        int root = getRoot();
        if (row > root) {
            setRoot(row);
        }
    }

    public static void main(String[] args) {
        Set<Integer> set = new HashSet<Integer>();
        int dataCount = 30;
        Random rd = new Random();
        int bound = dataCount * 3;
        while (set.size() < dataCount) {
            set.add(rd.nextInt(bound));
        }
        int i = 0;
        int topN = 5;
        int[] data = new int[topN];
        Iterator<Integer> it = set.iterator();
        while (i < topN) {
            data[i++] = it.next();
        }
        System.out.println(set);
        IntMinHeap heap = new IntMinHeap(data);
        heap.buildMinHeap();
        while (it.hasNext()) {
            heap.addIfRequired(it.next());
        }
        System.out.println(Arrays.toString(data));
    }
}
