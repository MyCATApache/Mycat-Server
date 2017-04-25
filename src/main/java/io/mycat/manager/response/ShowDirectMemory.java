package io.mycat.manager.response;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.NettyBufferPool;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.memory.MyCatMemory;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.utils.JavaUtils;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PoolSubpageMetric;
import sun.rmi.runtime.Log;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实现show@@directmemory功能
 *
 * @author zagnix
 * @version 1.0
 * @create 2016-09-21 17:35
 */

public class ShowDirectMemory {
    private static final int DETAILl_FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket detailHeader = PacketUtil.getHeader(DETAILl_FIELD_COUNT);
    private static final FieldPacket[] detailFields = new FieldPacket[DETAILl_FIELD_COUNT];
    private static final EOFPacket detailEof = new EOFPacket();


    private static final int TOTAL_FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket totalHeader = PacketUtil.getHeader(TOTAL_FIELD_COUNT);
    private static final FieldPacket[] totalFields = new FieldPacket[TOTAL_FIELD_COUNT];
    private static final EOFPacket totalEof = new EOFPacket();

    private static int useOffHeapForMerge ;
    private static int processorBufferPoolType;
    private static BufferPool bufferPool ;

    static {
        int i = 0;
        byte packetId = 0;
        detailHeader.packetId = ++packetId;

        detailFields[i] = PacketUtil.getField("THREAD_ID", Fields.FIELD_TYPE_VAR_STRING);
        detailFields[i++].packetId = ++packetId;

        detailFields[i] = PacketUtil.getField("MEM_USE_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        detailFields[i++].packetId = ++packetId;

        detailFields[i] = PacketUtil.getField("  SIZE  ", Fields.FIELD_TYPE_VAR_STRING);
        detailFields[i++].packetId = ++packetId;
        detailEof.packetId = ++packetId;


        i = 0;
        packetId = 0;

        totalHeader.packetId = ++packetId;

        totalFields[i] = PacketUtil.getField("MDIRECT_MEMORY_MAXED", Fields.FIELD_TYPE_VAR_STRING);
        totalFields[i++].packetId = ++packetId;

        totalFields[i] = PacketUtil.getField("DIRECT_MEMORY_USED", Fields.FIELD_TYPE_VAR_STRING);
        totalFields[i++].packetId = ++packetId;

        totalFields[i] = PacketUtil.getField("DIRECT_MEMORY_AVAILABLE", Fields.FIELD_TYPE_VAR_STRING);
        totalFields[i++].packetId = ++packetId;

        totalFields[i] = PacketUtil.getField("SAFETY_FRACTION", Fields.FIELD_TYPE_VAR_STRING);
        totalFields[i++].packetId = ++packetId;

        totalFields[i] = PacketUtil.getField("DIRECT_MEMORY_RESERVED", Fields.FIELD_TYPE_VAR_STRING);
        totalFields[i++].packetId = ++packetId;
        totalEof.packetId = ++packetId;

    }


    public static void execute(ManagerConnection c, int showtype) {
        useOffHeapForMerge = MycatServer.getInstance().getConfig().
                getSystem().getUseOffHeapForMerge();
        processorBufferPoolType = MycatServer.getInstance().getConfig().
                getSystem().getProcessorBufferPoolType();
        bufferPool = MycatServer.getInstance().getBufferPool();

        if (showtype == 1) {
            showDirectMemoryTotal(c);
        } else if (showtype == 2) {
            showDirectMemoryDetail(c);
        }
    }


    public static void showDirectMemoryDetail(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = detailHeader.write(buffer, c, true);

        // write fields
        for (FieldPacket field : detailFields) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = detailEof.write(buffer, c, true);

        // write rows
        byte packetId = detailEof.packetId;

        ConcurrentHashMap<Long, Long> bufferpoolUsageMap = bufferPool.getNetDirectMemoryUsage();

        try {

            if (useOffHeapForMerge == 1) {
                ConcurrentHashMap<Long, Long> concurrentHashMap = MycatServer.getInstance().
                        getMyCatMemory().
                        getResultMergeMemoryManager().getDirectMemorUsage();
                for (Long key : concurrentHashMap.keySet()) {


                    RowDataPacket row = new RowDataPacket(DETAILl_FIELD_COUNT);
                    Long value = concurrentHashMap.get(key);
                    row.add(String.valueOf(key).getBytes(c.getCharset()));
                    /**
                     * 该DIRECTMEMORY内存被结果集处理使用了
                     */
                    row.add("MergeMemoryPool".getBytes(c.getCharset()));
                    row.add(value > 0 ?
                            JavaUtils.bytesToString2(value).getBytes(c.getCharset()) : "0".getBytes(c.getCharset()));
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, c, true);
                }
            }

            if(processorBufferPoolType == 2){


            } else  {
                for (Long key : bufferpoolUsageMap.keySet()) {
                    RowDataPacket row = new RowDataPacket(DETAILl_FIELD_COUNT);
                    Long value = bufferpoolUsageMap.get(key);
                    row.add(String.valueOf(key).getBytes(c.getCharset()));
                    /**
                     * 该DIRECTMEMORY内存属于Buffer Pool管理的！
                     */
                    row.add("NetWorkBufferPool".getBytes(c.getCharset()));
                    row.add(value > 0 ?
                            JavaUtils.bytesToString2(value).getBytes(c.getCharset()) : "0".getBytes(c.getCharset()));
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, c, true);
                }
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);

    }


    public static void showDirectMemoryTotal(ManagerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        buffer = totalHeader.write(buffer, c, true);

        // write fields
        for (FieldPacket field : totalFields) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        buffer = totalEof.write(buffer, c, true);
        // write rows
        byte packetId = totalEof.packetId;

        long usedforMerge = 0;
        long usedforNetwork = 0;
        long chunkSizeBytes = 0;
        int chunkCount = 0;

        if (processorBufferPoolType == 2 && bufferPool instanceof NettyBufferPool) {

            /**计算逻辑就是，1.先计算PoolChunk分配的页,表示已经消耗的内存，
             2.然后计算小于一页情况，记录小于一页内存使用情况，
             上面二者合起来就是整个netty 使用的内存，
             已经分配了，但是没有使用的内存的情况*/
            List<PoolArenaMetric> list = ((NettyBufferPool) bufferPool).getAllocator().getAlloc().directArenas();
            chunkSizeBytes = ((NettyBufferPool) bufferPool).getAllocator().getChunkSize();
            long pageSize = ((NettyBufferPool) bufferPool).getAllocator().getPageSize();

            long chunksUsedBytes = 0;

            /**PoolArenas*/
            for (PoolArenaMetric pool : list) {
                List<PoolChunkListMetric> pcks = pool.chunkLists();

                /**针对PoolChunkList*/
                for (PoolChunkListMetric pck : pcks) {
                    Iterator<PoolChunkMetric> it = pck.iterator();
                    while (it.hasNext()) {
                        chunkCount++;
                        PoolChunkMetric p = it.next();
                        chunksUsedBytes += (chunkSizeBytes - p.freeBytes());
                    }
                }

                List<PoolSubpageMetric> tinySubpages = pool.tinySubpages();
                for (PoolSubpageMetric tiny : tinySubpages) {
                    chunksUsedBytes -= (pageSize - (tiny.maxNumElements() - tiny.numAvailable()) * tiny.elementSize());
                }
                List<PoolSubpageMetric> smallSubpages = pool.smallSubpages();
                for (PoolSubpageMetric small : smallSubpages) {
                    chunksUsedBytes -= (pageSize - (small.maxNumElements() - small.numAvailable()) * small.elementSize());
                }
            }

            usedforNetwork = chunkCount * chunkSizeBytes;
        }

        ConcurrentHashMap<Long, Long> bufferpoolUsageMap = bufferPool.getNetDirectMemoryUsage();

        RowDataPacket row = new RowDataPacket(TOTAL_FIELD_COUNT);

        try {

            /**
             * 通过-XX:MaxDirectMemorySize=2048m设置的值
             */
            row.add(JavaUtils.bytesToString2(Platform.getMaxDirectMemory()).getBytes(c.getCharset()));

            if (useOffHeapForMerge == 1) {

                /**
                 * 结果集合并时，总共消耗的DirectMemory内存
                 */
                ConcurrentHashMap<Long, Long> concurrentHashMap = MycatServer.getInstance().
                        getMyCatMemory().
                        getResultMergeMemoryManager().getDirectMemorUsage();
                for (Map.Entry<Long, Long> entry : concurrentHashMap.entrySet()) {
                    usedforMerge += entry.getValue();
                }
            }

            /**
             * 网络packet处理，在buffer pool 已经使用DirectMemory内存
             */
            if (processorBufferPoolType == 2) {
                usedforNetwork = chunkSizeBytes * chunkCount;
            } else {
                for (Map.Entry<Long, Long> entry : bufferpoolUsageMap.entrySet()) {
                    usedforNetwork += entry.getValue();
                }
            }

            row.add(JavaUtils.bytesToString2(usedforMerge + usedforNetwork).getBytes(c.getCharset()));


            long totalAvailable = 0;

            if (useOffHeapForMerge == 1) {
                /**
                 * 设置使用off-heap内存处理结果集时，防止客户把MaxDirectMemorySize设置到物理内存的极限。
                 * Mycat能使用的DirectMemory是MaxDirectMemorySize*DIRECT_SAFETY_FRACTION大小，
                 * DIRECT_SAFETY_FRACTION为安全系数，为OS，Heap预留空间，避免因大结果集造成系统物理内存被耗尽！
                 */
                totalAvailable = (long) (Platform.getMaxDirectMemory() * MyCatMemory.DIRECT_SAFETY_FRACTION);
            } else {
                totalAvailable = Platform.getMaxDirectMemory();
            }

            row.add(JavaUtils.bytesToString2(totalAvailable - usedforMerge - usedforNetwork)
                    .getBytes(c.getCharset()));

            if (useOffHeapForMerge == 1) {
                /**
                 * 输出安全系统DIRECT_SAFETY_FRACTION
                 */
                row.add(("" + MyCatMemory.DIRECT_SAFETY_FRACTION)
                        .getBytes(c.getCharset()));
            } else {
                row.add(("1.0")
                        .getBytes(c.getCharset()));
            }


            long resevedForOs = 0;

            if (useOffHeapForMerge == 1) {
                /**
                 * 预留OS系统部分内存！！！
                 */
                resevedForOs = (long) ((1 - MyCatMemory.DIRECT_SAFETY_FRACTION) *
                        (Platform.getMaxDirectMemory() -
                                2 * MycatServer.getInstance().getTotalNetWorkBufferSize()));
            }

            row.add(resevedForOs > 0 ? JavaUtils.bytesToString2(resevedForOs).getBytes(c.getCharset()) : "0".getBytes(c.getCharset()));

            row.packetId = ++packetId;
            buffer = row.write(buffer, c, true);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);

    }


}
