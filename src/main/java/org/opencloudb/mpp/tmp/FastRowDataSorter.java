package org.opencloudb.mpp.tmp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.opencloudb.mpp.OrderCol;
import org.opencloudb.net.mysql.RowDataPacket;

/**
 * RowDataSorter
 * 
 * @author czp:2014年12月8日
 *
 */
public class FastRowDataSorter implements MutilNodeMergeItf {

    private int totoal;
    private int colIndex;
    private int fieldCount;
    private MinHeap minHeap;
    protected RowDataCmp cmp;
    private List<byte[]> topn;
    protected OrderCol[] orderCols;
    private MemMapBytesArray rows;

    public FastRowDataSorter(OrderCol[] orderCols) {
        this.orderCols = orderCols;
        this.cmp = new RowDataCmp(orderCols);
        this.rows = new MemMapBytesArray(SWAP_PATH);
        this.colIndex = orderCols[0].colMeta.colIndex;
    }

    public void setLimit(int start, int size) {
        this.totoal = start + size;
        topn = new ArrayList<byte[]>(totoal);
    }

    // 为了避免数据拷贝,这里记录每条数据的fieldCount
    // 这里的fieldCount应该每条数据都一致
    // 只保留第一个排序字段在topn
    public void addRow(RowDataPacket row) {
        this.fieldCount = row.fieldCount;
        byte[] data = row.fieldValues.get(colIndex);
        if (topn.size() <= totoal) {
            topn.add(data);
            rows.add(row.value);
            if (topn.size() == totoal) {
                minHeap = new MinHeap(topn, cmp, orderCols[0]);
            }
            return;
        }
        int cmpRes = cmp.compareObject(data, minHeap.getRoot(), orderCols[0]);
        if (cmpRes > 0) {
            minHeap.setRoot(data);
            rows.add(row.value);
            return;
        }
        if (cmpRes == 0)
            rows.add(row.value);

    }

    public Collection<RowDataPacket> getResult() {
        topn.clear();// for gc
        System.out.println("----------->" + rows.size());
        MemMapSorter.MERGE_SORTER.sort(rows, new Comparator<byte[]>() {

            @Override
            public int compare(byte[] arg0, byte[] arg1) {
                RowDataPacket r1 = new RowDataPacket(fieldCount);
                RowDataPacket r2 = new RowDataPacket(fieldCount);
                r1.read(arg0);
                r2.read(arg1);
                return cmp.compare(r1, r2);
            }
        });
        return new CollectionWarpper(rows, fieldCount);
    }

    // 必须在finally里调用此方法
    @Override
    public void close() {
        rows.release();
    }

}
