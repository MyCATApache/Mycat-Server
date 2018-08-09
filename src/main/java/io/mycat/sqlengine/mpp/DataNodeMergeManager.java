package io.mycat.sqlengine.mpp;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.BufferUtil;
import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.backend.mysql.nio.handler.MultiNodeQueryHandler;
import io.mycat.memory.MyCatMemory;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.memory.mm.MemoryManager;
import io.mycat.memory.unsafe.row.BufferHolder;
import io.mycat.memory.unsafe.row.StructType;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.memory.unsafe.row.UnsafeRowWriter;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import io.mycat.memory.unsafe.utils.sort.PrefixComparator;
import io.mycat.memory.unsafe.utils.sort.PrefixComparators;
import io.mycat.memory.unsafe.utils.sort.RowPrefixComputer;
import io.mycat.memory.unsafe.utils.sort.UnsafeExternalRowSorter;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 数据节点合并
 * Created by zagnix on 2016/6/21.
 */
public class DataNodeMergeManager extends AbstractDataNodeMerge {

    private static Logger LOGGER = LoggerFactory.getLogger(DataNodeMergeManager.class);

    /**
     * key为datanode的分片节点名字
     * value为对应的排序器
     * 目前，没有使用！
     */
    private ConcurrentHashMap<String, UnsafeExternalRowSorter> unsafeRows = new ConcurrentHashMap<String,UnsafeExternalRowSorter>();
    /**
     * 全局sorter，排序器
     */
    private UnsafeExternalRowSorter globalSorter = null;
    /**
     * 非安全的行分组器
     */
    private UnsafeRowGrouper unsafeRowGrouper = null;

    /**
     * 全局合并，非安全的外部行排序器
     */
    private UnsafeExternalRowSorter globalMergeResult = null;

    /**
     * 排序器需要的上下文环境
     */
    private final MyCatMemory myCatMemory;
    private final MemoryManager memoryManager;
    private final MycatPropertyConf conf;
    /**
     * limit开始位置
     */
    private final  int limitStart;
    /**
     * limit的大小
     */
    private final  int limitSize;

    /**
     * 合并列索引
     */
    private int[] mergeColsIndex;
    private boolean hasEndFlag = false;

    private AtomicBoolean isMiddleResultDone;

    private UnsafeRow unsafeRow = null;
    private BufferHolder bufferHolder = null;
    private UnsafeRowWriter unsafeRowWriter = null;
    private int Index = 0;

    public DataNodeMergeManager(MultiNodeQueryHandler handler, RouteResultset rrs,AtomicBoolean isMiddleResultDone) {
        super(handler,rrs);
        this.isMiddleResultDone = isMiddleResultDone;
        this.myCatMemory = MycatServer.getInstance().getMyCatMemory();
        this.memoryManager = myCatMemory.getResultMergeMemoryManager();
        this.conf = myCatMemory.getConf();
        this.limitStart = rrs.getLimitStart();
        this.limitSize = rrs.getLimitSize();
    }

    public void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount) throws IOException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("field metadata keys:{}", columToIndx != null ? columToIndx.keySet() : "null");
            LOGGER.debug("field metadata values:{}", columToIndx != null ? columToIndx.values() : "null");
        }

        OrderCol[] orderCols = null;
        StructType schema = null;
        UnsafeExternalRowSorter.PrefixComputer prefixComputer = null;
        PrefixComparator prefixComparator = null;


        DataNodeMemoryManager dataNodeMemoryManager = null;
        UnsafeExternalRowSorter sorter = null;

        int[] groupColumnIndexs = null;
        this.fieldCount = fieldCount;

        if (rrs.getGroupByCols() != null) {
            // 有 group by
            groupColumnIndexs = toColumnIndex(rrs.getGroupByCols(), columToIndx);
            if (LOGGER.isDebugEnabled()) {
                for (int i = 0; i <rrs.getGroupByCols().length ; i++) {
                    LOGGER.debug("groupColumnIndexs:" + rrs.getGroupByCols()[i]);
                }
            }
        }


        if (rrs.getHavingCols() != null) {
            // 有 having
            ColMeta colMeta = columToIndx.get(rrs.getHavingCols().getLeft().toUpperCase());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("getHavingCols:" + rrs.getHavingCols().toString());
            }

            /**
             * 将 mycat 中 sql： select avg(xxx) from t
             * 重写 为 select sum(xxx) AS AVG[0~9]SUM,count(xxx) AS AVG[0~9]COUNT from t
             *  或者 select avg(xxx)  AS xxx from t
             *  select sum(xxx) AS xxxSUM,count(xxx) AS xxxCOUNT from t
             * 目的是方便在合并每个节点的结果集时再计算
             */
            if (colMeta == null) {
                for (String key : columToIndx.keySet()) {
                    if (key.toUpperCase().endsWith("SUM")) {
                        colMeta = columToIndx.get(key);
                        break;
                    }
                }
            }

            if (colMeta != null) {
                rrs.getHavingCols().setColMeta(colMeta);
            }
        }

        if (rrs.isHasAggrColumn()) {
            // 有sql count列
            List<MergeCol> mergCols = new LinkedList<MergeCol>();
            Map<String, Integer> mergeColsMap = rrs.getMergeCols();

            if (mergeColsMap != null) {
                if (LOGGER.isDebugEnabled() && rrs.getMergeCols() != null) {
                    LOGGER.debug("isHasAggrColumn:" + rrs.getMergeCols().toString());
                }
                for (Map.Entry<String, Integer> mergEntry : mergeColsMap.entrySet()) {
                    String colName = mergEntry.getKey().toUpperCase();
                    int type = mergEntry.getValue();
                    if (MergeCol.MERGE_AVG == type) { // 平均值
                        ColMeta sumColMeta = columToIndx.get(colName + "SUM");
                        ColMeta countColMeta = columToIndx.get(colName + "COUNT");
                        if (sumColMeta != null && countColMeta != null) {
                            // 重新计算平均值
                            ColMeta colMeta = new ColMeta(sumColMeta.colIndex, countColMeta.colIndex, sumColMeta.getColType());
                            mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
                        }
                    } else {
                        ColMeta colMeta = columToIndx.get(colName);
                        mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
                    }
                }
            }

            // 不添加别名合并列
            for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
                String colName = fieldEntry.getKey();
                int result = MergeCol.tryParseAggCol(colName);
                if (result != MergeCol.MERGE_UNSUPPORT && result != MergeCol.MERGE_NOMERGE) {
                    mergCols.add(new MergeCol(fieldEntry.getValue(), result));
                }
            }

            /**
             * Group操作
             */
            MergeCol[] mergColsArrays = mergCols.toArray(new MergeCol[mergCols.size()]);
            unsafeRowGrouper = new UnsafeRowGrouper(columToIndx,rrs.getGroupByCols(),
                    mergColsArrays,
                    rrs.getHavingCols());

            if(mergColsArrays!=null&&mergColsArrays.length>0){
                mergeColsIndex = new int[mergColsArrays.length];
                for(int i = 0;i<mergColsArrays.length;i++){
                    mergeColsIndex[i] = mergColsArrays[i].colMeta.colIndex;
                }
                Arrays.sort(mergeColsIndex);
            }
        }


        if (rrs.getOrderByCols() != null) {
            // 有 order by
            LinkedHashMap<String, Integer> orders = rrs.getOrderByCols();
            orderCols = new OrderCol[orders.size()];
            int i = 0;
            for (Map.Entry<String, Integer> entry : orders.entrySet()) {
                String key = StringUtil.removeBackquote(entry.getKey().toUpperCase());
                ColMeta colMeta = columToIndx.get(key);
                if (colMeta == null) {
                    throw new IllegalArgumentException(
                            "all columns in order by clause should be in the selected column list!"
                                    + entry.getKey());
                }
                orderCols[i++] = new OrderCol(colMeta, entry.getValue());
            }

            /**
             * 构造全局排序器
             */
            schema = new StructType(columToIndx,fieldCount);
            schema.setOrderCols(orderCols);

            prefixComputer = new RowPrefixComputer(schema);

//            if(orderCols.length>0
//                    && orderCols[0].getOrderType()
//                    == OrderCol.COL_ORDER_TYPE_ASC){
//                prefixComparator = PrefixComparators.LONG;
//            }else {
//                prefixComparator = PrefixComparators.LONG_DESC;
//            }

            prefixComparator = getPrefixComparator(orderCols);

            dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager,Thread.currentThread().getId());

            /**
             * 默认排序，只是将数据连续存储到内存中即可。
             */
            globalSorter = new UnsafeExternalRowSorter(
                    dataNodeMemoryManager,
                    myCatMemory,
                    schema,
                    prefixComparator, prefixComputer,
                    conf.getSizeAsBytes("mycat.buffer.pageSize","32k"),
                    false/**是否使用基数排序*/,
                    true/**排序*/);
        }


        if(conf.getBoolean("mycat.stream.output.result",false)
                && globalSorter == null
                && unsafeRowGrouper == null){
            // 启用结果集流输出，不经过合并
            setStreamOutputResult(true);
        } else {
            /**
             * 1.schema
             */
            schema = new StructType(columToIndx,fieldCount);
            schema.setOrderCols(orderCols);

            /**
             * 2 .PrefixComputer
             */
            prefixComputer = new RowPrefixComputer(schema);

            /**
             * 3 .PrefixComparator 默认是ASC，可以选择DESC
             */
            prefixComparator = PrefixComparators.LONG;

            dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager, Thread.currentThread().getId());

            globalMergeResult = new UnsafeExternalRowSorter(
                    dataNodeMemoryManager,
                    myCatMemory,
                    schema,
                    prefixComparator,
                    prefixComputer,
                    conf.getSizeAsBytes("mycat.buffer.pageSize", "32k"),
                    false,/**是否使用基数排序*/
                    false/**不排序*/);
        }
    }

    /**
     * 根据排序列获取前缀比较器
     * @param orderCols
     * @return
     */
    private PrefixComparator getPrefixComparator(OrderCol[] orderCols) {
        PrefixComparator prefixComparator = null;
        OrderCol firstOrderCol = orderCols[0];
        int orderType = firstOrderCol.getOrderType();
        int colType = firstOrderCol.colMeta.colType;

        switch (colType) {
            case ColMeta.COL_TYPE_INT:
            case ColMeta.COL_TYPE_LONG:
            case ColMeta.COL_TYPE_INT24:
            case ColMeta.COL_TYPE_SHORT:
            case ColMeta.COL_TYPE_LONGLONG:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ? PrefixComparators.LONG : PrefixComparators.LONG_DESC);
                break;
            case ColMeta.COL_TYPE_FLOAT:
            case ColMeta.COL_TYPE_DOUBLE:
            case ColMeta.COL_TYPE_DECIMAL:
            case ColMeta.COL_TYPE_NEWDECIMAL:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ? PrefixComparators.DOUBLE : PrefixComparators.DOUBLE_DESC);
                break;
            case ColMeta.COL_TYPE_DATE:
            case ColMeta.COL_TYPE_TIMSTAMP:
            case ColMeta.COL_TYPE_TIME:
            case ColMeta.COL_TYPE_YEAR:
            case ColMeta.COL_TYPE_DATETIME:
            case ColMeta.COL_TYPE_NEWDATE:
            case ColMeta.COL_TYPE_BIT:
            case ColMeta.COL_TYPE_VAR_STRING:
            case ColMeta.COL_TYPE_STRING:
            case ColMeta.COL_TYPE_ENUM:
            case ColMeta.COL_TYPE_SET:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ? PrefixComparators.BINARY : PrefixComparators.BINARY_DESC);
                break;
            default:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ? PrefixComparators.LONG : PrefixComparators.LONG_DESC);
                break;
        }

        return prefixComparator;
    }

    @Override
    public List<RowDataPacket> getResults(byte[] eof) {
        return null;
    }

    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        /**
         * 是否是空包
         */
        boolean nulpack = false;

        try {
            for (; ; ) {
                final PackWraper pack = packs.poll();

                if (pack == null) {
                    // 空包跳出循环 表示数据接收完
                    nulpack = true;
                    break;
                }
                if (pack == END_FLAG_PACK) {
                    // 结束包
                    hasEndFlag = true;
                    if(packs.peek()!=null){
                        // 结束包的下一个包不是空，将结束包放回队列，并跳过以下操作
                        packs.add(pack);
                        continue;
                    }

                    /**
                     * 最后一个节点datenode发送了row eof packet说明了整个分片数据全部接收完成，进而将结果集全部发给Mycat客户端
                     */
                    final int warningCount = 0;
                    final EOFPacket eofp = new EOFPacket();
                    final ByteBuffer eof = ByteBuffer.allocate(9);
                    BufferUtil.writeUB3(eof, eofp.calcPacketSize());
                    eof.put(eofp.packetId);
                    eof.put(eofp.fieldCount);
                    BufferUtil.writeUB2(eof,warningCount);
                    BufferUtil.writeUB2(eof,eofp.status);
                    final ServerConnection source = multiQueryHandler.getSession().getSource();
                    final byte[] array = eof.array();

                    Iterator<UnsafeRow> iters = null;

                    if (unsafeRowGrouper != null){
                        /**
                         * group by里面需要排序情况
                         */
                        if (globalSorter != null){
                            iters = unsafeRowGrouper.getResult(globalSorter);
                        } else if(globalMergeResult != null){
                            iters = unsafeRowGrouper.getResult(globalMergeResult);
                        }
                    }else if(globalSorter != null){
                        iters = globalSorter.sort();
                    }else if (globalMergeResult != null){
                        iters = globalMergeResult.sort();
                    }

                    if(iters != null){
                        multiQueryHandler.outputMergeResult(source,array,iters,isMiddleResultDone);
                    }
                    // 跳出循环
                    break;
                }

                // 处理队列的数据包

                unsafeRow = new UnsafeRow(fieldCount);
                bufferHolder = new BufferHolder(unsafeRow,0);
                unsafeRowWriter = new UnsafeRowWriter(bufferHolder,fieldCount);
                bufferHolder.reset();

                /**
                 *构造一行row，将对应的col填充.
                 */
                MySQLMessage mm = new MySQLMessage(pack.rowData);
                mm.readUB3();
                mm.read();

                int nullnum = 0;
                for (int i = 0; i < fieldCount; i++) {
                    byte[] colValue = mm.readBytesWithLength(); //如果mysql的表结构不一致，会导致数据解析有问题
                    if (colValue != null) {
                        unsafeRowWriter.write(i, colValue);
                    } else {
                        if(mergeColsIndex!=null&&mergeColsIndex.length>0){
                            if(Arrays.binarySearch(mergeColsIndex, i)<0){
                                nullnum++;
                            }
                        }
                        unsafeRow.setNullAt(i);
                    }
                }

                if(mergeColsIndex!=null&&mergeColsIndex.length>0){
                    if(nullnum == (fieldCount - mergeColsIndex.length)){
                        if(!hasEndFlag){
                            packs.add(pack);
                            continue;
                        }
                    }
                }

                unsafeRow.setTotalSize(bufferHolder.totalSize());

                if(unsafeRowGrouper != null){
                    unsafeRowGrouper.addRow(unsafeRow);
                }else if (globalSorter != null){
                    globalSorter.insertRow(unsafeRow);
                }else if (globalMergeResult != null){
                    globalMergeResult.insertRow(unsafeRow);
                }
                unsafeRow = null;
                bufferHolder = null;
                unsafeRowWriter = null;
            }

        } catch (final Exception e) {
            e.printStackTrace();
            multiQueryHandler.handleDataProcessException(e);
        } finally {
            running.set(false);
            if (nulpack && !packs.isEmpty()) {
                this.run();
            }
        }
    }

    /**
     * 释放DataNodeMergeManager所申请的资源
     */
    public void clear() {

        unsafeRows.clear();

        synchronized (this) {
            if (unsafeRowGrouper != null) {
                unsafeRowGrouper.free();
                unsafeRowGrouper = null;
            }
        }

        if(globalSorter != null){
            globalSorter.cleanupResources();
            globalSorter = null;
        }

        if (globalMergeResult != null){
            globalMergeResult.cleanupResources();
            globalMergeResult = null;
        }
    }
}
