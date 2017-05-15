package io.mycat.sqlengine.mpp.tmp;

import java.util.Comparator;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.sqlengine.mpp.OrderCol;
import io.mycat.sqlengine.mpp.RowDataPacketSorter;

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
			// fix bug 当 order by 列 为  null 时, 报空指针的异常.
			if(left==null){ left = new byte[0];}
			if(right==null){ right = new byte[0];}
			if (tmp[i].orderType == type) {
				cmp = RowDataPacketSorter.compareObject(left, right, tmp[i]);
			} else {
				cmp = RowDataPacketSorter.compareObject(right, left, tmp[i]);
			}
			if (cmp != 0) {
				return cmp;
			}
		}
		return cmp;
	}

}
