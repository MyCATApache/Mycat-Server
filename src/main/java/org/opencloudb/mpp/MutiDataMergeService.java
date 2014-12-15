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
package org.opencloudb.mpp;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.mpp.controller.NodeExcutionController;
import org.opencloudb.mpp.model.NodeRowDataPacket;
import org.opencloudb.mysql.nio.handler.MultiNodeQueryWithLimitHandler;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;

/**
 * Data merge service handle data Min,Max,AVG group 、order by 、limit
 * 
 * @author wuzhih
 * 
 */
public class MutiDataMergeService extends DataMergeService implements ResponseHandler {
    private static final Logger LOGGER = Logger.getLogger(MutiDataMergeService.class);
    private RowDataPacketGrouper grouper = null;
    private RangRowDataPacketSorter sorter = null;
    private NodeExcutionController dataController;
    private MultiNodeQueryWithLimitHandler limitExcution = null;
    private Map<String, NodeRowDataPacket> result = new HashMap<String, NodeRowDataPacket>();
    private int pagePatchSize = 100;
    private int fieldCount;
    private final RouteResultset rrs;

    public MutiDataMergeService(RouteResultset rrs) {
        super(rrs);
        this.rrs = rrs;
        this.dataController = new NodeExcutionController(this.rrs, this.result, this);
        RouteResultsetNode[] nodeArr = this.rrs.getNodes();

        this.pagePatchSize = this.rrs.getLimitSize();
        SystemConfig sysConfig = MycatServer.getInstance().getConfig().getSystem();
        int mutiNodePatchSize = sysConfig.getMutiNodePatchSize();
        int size = ((mutiNodePatchSize - (mutiNodePatchSize % this.pagePatchSize)) / this.pagePatchSize + 1)
                * this.pagePatchSize;
        if (pagePatchSize < size) {
            pagePatchSize = size;
        }

        for (RouteResultsetNode node : nodeArr) {
            NodeRowDataPacket packet = new NodeRowDataPacket(node, pagePatchSize);
            result.put(node.getName(), packet);
        }
    }

    public void setLimitExcution(MultiNodeQueryWithLimitHandler limitExcution) {
        this.limitExcution = limitExcution;
    }

    public void initHandler(NonBlockingSession session) {
        this.dataController.initHandler(session);
    }

    private void appendNodeRang(String dataNode) {
        NodeRowDataPacket packet = result.get(dataNode);
        packet.newRang();
    }

    public void execute(BackendConnection conn, RouteResultsetNode node) {
        this.appendNodeRang(node.getName());
        this.dataController._execute(conn, node);
    }

    public void releaseAllBackend() {
        this.dataController.releaseAllBackend();
    }

    public long loadTrimTotal() {
        long trimTotal = 0;
        Set<String> dataNodeNameSet = result.keySet();
        for (Iterator<String> iter = dataNodeNameSet.iterator(); iter.hasNext();) {
            String dataNodeName = iter.next();
            NodeRowDataPacket nodePacket = result.get(dataNodeName);

            trimTotal += nodePacket.loadTrimTotal();
        }
        return trimTotal;
    }

    public void dataOk(String dataNode, byte[] eof, BackendConnection conn) {
        this.dataController.dataOk(dataNode, eof, conn);
    }

    /**
     * return merged data
     * 
     * @return
     */
    public Collection<RowDataPacket> getResults() {
        Collection<RowDataPacket> tmpResult = new LinkedList<RowDataPacket>();
        Set<String> dataNodeNameSet = result.keySet();
        for (Iterator<String> iter = dataNodeNameSet.iterator(); iter.hasNext();) {
            String dataNodeName = iter.next();
            NodeRowDataPacket nodePacket = result.get(dataNodeName);
            List<RowDataPacket> list = nodePacket.loadData();

            tmpResult.addAll(list);
        }

        if (this.grouper != null) {
            tmpResult = grouper.getResult();
        }
        if (sorter != null) {
            tmpResult = sorter.getSortedResult();
        }
        return tmpResult;
    }

    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }

    public RouteResultset getRrs() {
        return rrs;
    }

    public void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("field metadata inf:" + Arrays.toString(columToIndx.entrySet().toArray()));
        }
        int[] groupColumnIndexs = null;
        this.fieldCount = fieldCount;
        if (rrs.getGroupByCols() != null) {
            groupColumnIndexs = (toColumnIndex(rrs.getGroupByCols(), columToIndx));
        }
        if (rrs.isHasAggrColumn()) {
            List<MergeCol> mergCols = new LinkedList<MergeCol>();
            if (rrs.getMergeCols() != null) {
                for (Map.Entry<String, Integer> mergEntry : rrs.getMergeCols().entrySet()) {
                    String colName = mergEntry.getKey().toUpperCase();
                    ColMeta colMeta = columToIndx.get(colName);
                    mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
                }
            }
            // add no alias merg column
            for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
                String colName = fieldEntry.getKey();
                int result = MergeCol.tryParseAggCol(colName);
                if (result != MergeCol.MERGE_UNSUPPORT && result != MergeCol.MERGE_NOMERGE) {
                    mergCols.add(new MergeCol(fieldEntry.getValue(), result));
                }
            }
            grouper = new RowDataPacketGrouper(groupColumnIndexs, mergCols.toArray(new MergeCol[mergCols.size()]));
        }
        if (rrs.getOrderByCols() != null) {
            LinkedHashMap<String, Integer> orders = rrs.getOrderByCols();
            OrderCol[] orderCols = new OrderCol[orders.size()];
            int i = 0;
            for (Map.Entry<String, Integer> entry : orders.entrySet()) {
                orderCols[i++] = new OrderCol(columToIndx.get(entry.getKey().toUpperCase()), entry.getValue());
            }
            sorter = new RangRowDataPacketSorter(orderCols);
            this.dataController.setSorter(sorter);
        }
    }

    public void onNewRangRecord(String dataNode) {
        NodeRowDataPacket nodePacket = this.result.get(dataNode);
        nodePacket.newRang();
    }

    /**
     * process new record (mysql binary data),if data can output to client
     * ,return true
     * 
     * @param dataNode
     *            DN's name (data from this dataNode)
     * @param rowData
     *            raw data
     */
    public boolean onNewRecord(String dataNode, byte[] rowData) {
        NodeRowDataPacket nodePacket = this.result.get(dataNode);
        RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
        rowDataPkg.read(rowData);
        if (grouper != null) {
            grouper.addRow(rowDataPkg);
        } else {
            nodePacket.addPacket(rowDataPkg);
            this.dataController.newRecord(dataNode);
        }
        return false;

    }

    private static int[] toColumnIndex(String[] columns, Map<String, ColMeta> toIndexMap) {
        int[] result = new int[columns.length];
        ColMeta curColMeta = null;
        for (int i = 0; i < columns.length; i++) {
            curColMeta = toIndexMap.get(columns[i].toUpperCase());
            if (curColMeta == null) {
                throw new java.lang.IllegalArgumentException("can't find column in select fields " + columns[i]);
            }
            result[i] = curColMeta.colIndex;
        }
        return result;
    }

    /**
     * release resources
     */
    public void clear() {
        grouper = null;
        sorter = null;
        result = null;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        this.limitExcution.connectionAcquired(conn);
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        this.limitExcution.connectionClose(conn, reason);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        this.limitExcution.connectionError(e, conn);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        this.limitExcution.errorResponse(err, conn);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
        this.limitExcution.fieldEofResponse(header, fields, eof, conn);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        this.limitExcution.okResponse(ok, conn);
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        this.limitExcution.rowEofResponse(eof, conn);
    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        this.limitExcution.rowResponse(row, conn);
    }

    @Override
    public void writeQueueAvailable() {
        this.limitExcution.writeQueueAvailable();
    }

    public int getPagePatchSize() {
        return pagePatchSize;
    }

}