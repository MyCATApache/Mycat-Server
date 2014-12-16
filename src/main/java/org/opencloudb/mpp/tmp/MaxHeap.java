package org.opencloudb.mpp.tmp;

import java.util.Vector;

import org.opencloudb.net.mysql.RowDataPacket;

/**
 * 
 * @author coderczp-2014-12-8
 */
public class MaxHeap implements HeapItf {

    private RowDataCmp cmp;
    private Vector<RowDataPacket> data;

    public MaxHeap(RowDataCmp cmp, int size) {
        this.cmp = cmp;
        this.data = new Vector<RowDataPacket>();
    }

    @Override
    public void buildMinHeap() {
        int len = data.size();
        for (int i = len / 2 - 1; i >= 0; i--) {
            heapify(i);
        }
    }

    private void heapify(int i) {
        int l = left(i);
        int r = right(i);
        int max = i;
        int len = data.size();
        if (l < len && cmp.compare(data.elementAt(l), data.elementAt(i)) > 0)
            max = l;
        if (r < len && cmp.compare(data.elementAt(r), data.elementAt(max)) > 0)
            max = r;
        if (i == max)
            return;
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
        RowDataPacket tmp = data.elementAt(i);
        RowDataPacket elementAt = data.elementAt(j);
        data.set(i, elementAt);
        data.set(j, tmp);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.opencloudb.mpp.tmp.HeapItf#getRoot()
     */
    @Override
    public RowDataPacket getRoot() {
        return data.elementAt(0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.opencloudb.mpp.tmp.HeapItf#setRoot(org.opencloudb.net.mysql.RowDataPacket)
     */
    @Override
    public void setRoot(RowDataPacket root) {
        data.set(0, root);
        heapify(0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.opencloudb.mpp.tmp.HeapItf#getData()
     */
    @Override
    public Vector<RowDataPacket> getData() {
        return data;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.opencloudb.mpp.tmp.HeapItf#add(org.opencloudb.net.mysql.RowDataPacket)
     */
    @Override
    public synchronized void add(RowDataPacket row) {
        data.add(row);
    }

}
