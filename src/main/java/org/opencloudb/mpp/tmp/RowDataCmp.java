package org.opencloudb.mpp.tmp;

import java.util.Comparator;

import org.opencloudb.mpp.ColMeta;
import org.opencloudb.mpp.OrderCol;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.ByteUtil;
import org.opencloudb.util.CompareUtil;

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
        int type = OrderCol.COL_ORDER_TYPE_ASC;
        for (int i = 0; i < len; i++) {
            int colIndex = tmp[i].colMeta.colIndex;
            byte[] left = o1.fieldValues.get(colIndex);
            byte[] right = o2.fieldValues.get(colIndex);
            if (tmp[i].orderType == type) {
                cmp = compareObject(left, right, tmp[i]);
            } else {
                cmp = compareObject(right, left, tmp[i]);
            }
            if (cmp != 0)
                return cmp;
        }
        return cmp;
    }

    public int compareObject(byte[] left, byte[] right, OrderCol orderCol) {
        /*
         * mysql的日期也是数字字符串方式表达，因此可以跟整数等一起对待 BLOB相关类型和GEOMETRY类型不支持排序，略掉
         * ENUM和SET类型都是字符串，按字符串处理
         */
        switch (orderCol.getColMeta().getColType()) {
        case ColMeta.COL_TYPE_INT:
        case ColMeta.COL_TYPE_LONG:
        case ColMeta.COL_TYPE_SHORT:
        case ColMeta.COL_TYPE_FLOAT:
        case ColMeta.COL_TYPE_INT24:
        case ColMeta.COL_TYPE_DOUBLE:
        case ColMeta.COL_TYPE_DECIMAL:
        case ColMeta.COL_TYPE_LONGLONG:
        case ColMeta.COL_TYPE_NEWDECIMAL:
        case ColMeta.COL_TYPE_BIT:
        case ColMeta.COL_TYPE_DATE:
        case ColMeta.COL_TYPE_TIME:
        case ColMeta.COL_TYPE_YEAR:
        case ColMeta.COL_TYPE_NEWDATE:
        case ColMeta.COL_TYPE_DATETIME:
        case ColMeta.COL_TYPE_TIMSTAMP:
            return ByteUtil.compareNumberByte(left, right);
        case ColMeta.COL_TYPE_STRING:
        case ColMeta.COL_TYPE_VAR_STRING:
        case ColMeta.COL_TYPE_SET:
        case ColMeta.COL_TYPE_ENUM:
            return CompareUtil.compareString(ByteUtil.getString(left), ByteUtil.getString(right));
        }
        return 0;
    }
}