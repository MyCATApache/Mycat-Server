package org.opencloudb.mpp.tmp;

import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.RowDataPacketSorter;
import org.opencloudb.net.mysql.RowDataPacket;

import java.util.Comparator;

/**
 * 
 * @author coderczp-2014-12-8
 */
public class RowDataCmp implements Comparator<RowDataPacket> {

	private OrderCol[] orderCols;

	public RowDataCmp(OrderCol[] orderCols) {
		this.orderCols = orderCols;
	}

	@Override
	public int compare(RowDataPacket o1, RowDataPacket o2) {
		OrderCol[] tmp = this.orderCols;
		int cmp = 0;
		int len = tmp.length;
		//依次比较order by语句上的多个排序字段的值
		int type = OrderCol.COL_ORDER_TYPE_ASC;
		for (int i = 0; i < len; i++) {
			int colIndex = tmp[i].colMeta.colIndex;
			byte[] left = o1.fieldValues.get(colIndex);
			byte[] right = o2.fieldValues.get(colIndex);
			if (tmp[i].orderType == type) {
				cmp = RowDataPacketSorter.compareObject(left, right, tmp[i]);
			} else {
				cmp = RowDataPacketSorter.compareObject(right, left, tmp[i]);
			}
			if (cmp != 0)
				return cmp;
		}
		return cmp;
	}

}
