/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.mpp.tmp;

import java.util.Arrays;
import java.util.Collection;

import org.opencloudb.mpp.ColMeta;
import org.opencloudb.mpp.MergeCol;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.ByteUtil;
import org.opencloudb.util.LongUtil;

/**
 * implement group function select a,count(*),sum(*) from A group by a
 * 
 * @author coderczp 2014-12-11
 * 
 */
public class RowDataPacketGrouper implements MutilNodeMergeItf {

    private int fieldCount;
    private MergeCol[] mergCols;
    private MemMapBytesArray rows;
    private int[] groupColumnIndexs;

    public RowDataPacketGrouper(int[] groupColumnIndexs, MergeCol[] mergCols) {
        this.mergCols = mergCols;
        this.groupColumnIndexs = groupColumnIndexs;
        this.rows = new MemMapBytesArray(SWAP_PATH);
    }

    public Collection<RowDataPacket> getResult() {
        return new CollectionWarpper(rows, fieldCount);
    }

    public void addRow(RowDataPacket rowDataPkg) {
        fieldCount = rowDataPkg.fieldCount;
        for (byte[] bs : rows) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            row.read(bs);
            if (sameGropuColums(rowDataPkg, row)) {
                aggregateRow(row, rowDataPkg);
                return;
            }
        }
        // not aggreated ,insert new
        rows.add(rowDataPkg.value);

    }

    private void aggregateRow(RowDataPacket toRow, RowDataPacket newRow) {
        if (mergCols == null) {
            return;
        }
        for (MergeCol merg : mergCols) {

            byte[] result = mertFields(toRow.fieldValues.get(merg.colMeta.colIndex),
                    newRow.fieldValues.get(merg.colMeta.colIndex), merg.colMeta.colType, merg.mergeType);
            if (result != null) {
                toRow.fieldValues.set(merg.colMeta.colIndex, result);
            }
        }

    }

    private byte[] mertFields(byte[] bs, byte[] bs2, int colType, int mergeType) {
        if (bs2.length == 0) {
            return bs;
        } else if (bs.length == 0) {
            return bs2;
        }
        switch (mergeType) {
        case MergeCol.MERGE_SUM:
            if (colType == ColMeta.COL_TYPE_NEWDECIMAL || colType == ColMeta.COL_TYPE_DOUBLE
                    || colType == ColMeta.COL_TYPE_FLOAT) {

                Double vale = ByteUtil.getDouble(bs) + ByteUtil.getDouble(bs2);
                return vale.toString().getBytes();
            }
            // continue to count case
        case MergeCol.MERGE_COUNT: {
            long s1 = Long.parseLong(new String(bs));
            long s2 = Long.parseLong(new String(bs2));
            long total = s1 + s2;
            return LongUtil.toBytes(total);
        }
        case MergeCol.MERGE_MAX: {
            int compare = ByteUtil.compareNumberByte(bs, bs2);
            return (compare > 0) ? bs : bs2;

        }
        case MergeCol.MERGE_MIN: {
            int compare = ByteUtil.compareNumberByte(bs, bs2);
            return (compare > 0) ? bs2 : bs;
        }
        default:
            return null;
        }

    }

    private boolean sameGropuColums(RowDataPacket newRow, RowDataPacket existRow) {
        if (groupColumnIndexs == null) {// select count(*) from aaa , or group
                                        // column
            return true;
        }
        for (int i = 0; i < groupColumnIndexs.length; i++) {
            if (!Arrays.equals(newRow.fieldValues.get(groupColumnIndexs[i]),
                    existRow.fieldValues.get(groupColumnIndexs[i]))) {
                return false;
            }

        }
        return true;

    }

    @Override
    public void close() {
        if (rows != null)
            rows.release();
    }
}