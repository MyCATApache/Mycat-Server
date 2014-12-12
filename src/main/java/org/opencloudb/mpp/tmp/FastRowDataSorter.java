package org.opencloudb.mpp.tmp;

import java.util.Collection;
import java.util.Comparator;

import org.opencloudb.mpp.OrderCol;
import org.opencloudb.net.mysql.RowDataPacket;

/**
 * RowDataSorter
 * 
 * @author czp:2014年12月8日
 *
 */
public class FastRowDataSorter implements MutilNodeMergeItf {

	private int fieldCount;
	protected RowDataCmp cmp;
	protected OrderCol[] orderCols;
	private MemMapBytesArray rows;
	public static final String SWAP_PATH = "./";

	public FastRowDataSorter(OrderCol[] orderCols) {
		this.orderCols = orderCols;
		this.cmp = new RowDataCmp(orderCols);
		this.rows = new MemMapBytesArray(SWAP_PATH);
	}

	public void addRow(RowDataPacket row) {
		//为了避免数据拷贝,这里记录每条数据的fieldCount
		//这里的fieldCount应该每条数据都一致
		this.fieldCount = row.fieldCount;
		rows.add(row.value);

	}

	public Collection<RowDataPacket> getResult() {
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

	//必须在finally里调用此方法
	@Override
	public void close() {
		rows.release();
	}

}
