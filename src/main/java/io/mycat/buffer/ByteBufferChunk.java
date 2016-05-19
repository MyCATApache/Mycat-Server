package io.mycat.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * 仿照Netty的思路，针对MyCat内存缓冲策略优化
 * Chunk由Page组成，是一块连续内存，由memoryMap和depthMap定义成一种平衡二叉树的管理结构
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 17:19 2016/5/17
 * @see @https://github.com/netty/netty
 */
public class ByteBufferChunk implements Comparable{
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferChunk.class);
    private final byte[] memoryMap;
    private final byte[] depthMap;
    private final ByteBuffer buf;

    //in bytes
    private final int pageSize;
    //in bytes
    private final int chunkSize;
    private final int chunkPageSize;
    private final int maxOrder;
    private final byte unusable;
    private final int log2PageSize;
    final long bufAddress;

    private int freeBytes;

    ByteBufferChunk prev;
    ByteBufferChunk next;
    ByteBufferChunkList parent;

    public ByteBufferChunk(int pageSize, int chunkSize) {

        this.pageSize = pageSize;
        this.chunkSize = chunkSize;
        this.chunkPageSize = chunkSize / pageSize;
        this.maxOrder = log2(this.chunkPageSize) + 1;
        this.unusable = (byte) this.maxOrder;
        this.freeBytes = chunkSize;
        this.buf = ByteBuffer.allocateDirect(chunkSize);
        this.bufAddress = ((DirectBuffer) buf).address();

        this.depthMap = new byte[(1 << this.maxOrder)];
        this.memoryMap = new byte[this.depthMap.length];

        this.log2PageSize = log2(pageSize);

        int memoryMapIndex = 1;
        for (int d = 0; d < maxOrder; ++d) { // move down the tree one level at a time
            int depth = 1 << d;
            for (int p = 0; p < depth; ++p) {
                // in each level traverse left to right and set value to the depth of subtree
                memoryMap[memoryMapIndex] = (byte) d;
                depthMap[memoryMapIndex] = (byte) d;
                memoryMapIndex++;
            }
        }
    }

    public boolean isInThisChunk(ByteBuffer byteBuffer) {
        long address = ((DirectBuffer) byteBuffer).address();
        return (address >= bufAddress) && (address < bufAddress + chunkSize);
    }

    public int usage() {
        final int freeBytes = this.freeBytes;
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    public synchronized ByteBuffer allocateRun(int normCapacity) {
        if(normCapacity > chunkSize){
            LOGGER.warn("try to acquire a buffer with larger size than chunkSize!");
            return null;
        }
        int d = this.maxOrder - 2 - (log2(normCapacity) - this.log2PageSize);
        if (d > this.maxOrder - 1) {
            d = maxOrder - 1;
        }
        int id = allocateNode(d);
        if (id < 0) {
            return null;
        }
        freeBytes -= runLength(id);

        int start = calculateStart(id);
        int end = start + runLength(id);

        buf.limit(end);
        buf.position(start);

//        printMemoryMap();

        return buf.slice();
    }


    private int calculateStart(int id) {
        int count = 0;
        for (int i = 1; i < depthMap.length; i++) {
            if (depthMap[i] < depthMap[id]) {
                continue;
            } else if (depthMap[i] == depthMap[id]) {
                if (i == id) {
                    break;
                } else {
                    count += runLength(i);
                }
            } else {
                break;
            }
        }
        return count;
    }

    private int runLength(int id) {
        // represents the size in #bytes supported by node 'id' in the tree
        return 1 << log2(chunkSize) - depthMap[id];
    }

    private int allocateNode(int d) {
        int id = 1;
        int initial = -(1 << d); // has last d bits = 0 and rest all = 1
        byte val = memoryMap[id];
        if (val > d) { // unusable
            return -1;
        }

        while (val < d || (id & initial) == 0) { // id & initial == 1 << d for all ids at depth d, for < d it is 0
            id <<= 1;
            val = memoryMap[id];
            if (val > d) {
                id ^= 1;
                val = memoryMap[id];
            }
        }
        byte value = memoryMap[id];
        assert value == d && (id & initial) == 1 << d : String.format("val = %d, id & initial = %d, d = %d",
                value, id & initial, d);
        memoryMap[id] = unusable; // mark as unusable
        updateParentsAlloc(id);
        return id;
    }

    private void updateParentsAlloc(int id) {
        while (id > 1) {
            int parentId = id >>> 1;
            byte val1 = memoryMap[id];
            byte val2 = memoryMap[id ^ 1];
            byte val = val1 < val2 ? val1 : val2;
            memoryMap[parentId] = val;
            id = parentId;
        }
    }

    public synchronized void freeByteBuffer(ByteBuffer byteBuffer) {
        long address = ((DirectBuffer) byteBuffer).address();
        int relativeAddress = (int) (address - bufAddress);
        int length = byteBuffer.capacity();

        int depth = maxOrder - 1 - log2(length / pageSize);
        int count = 0;
        int i;
        for (i = 0; i < depthMap.length; i++) {
            if (depthMap[i] == depth) {
                if (count == relativeAddress) {
                    break;
                }
                count += length;
            }
            if (depthMap[i] > depth) {
                break;
            }
        }
        free(i);
    }

    private void free(int handle) {
        if (memoryMap[handle] != depthMap[handle]) {
            freeBytes += runLength(handle);
            memoryMap[handle] = depthMap[handle];
            updateParentsFree(handle);
        }
    }

    private void updateParentsFree(int id) {
        int logChild = depthMap[id] + 1;
        while (id > 1) {
            int parentId = id >>> 1;
            byte val1 = memoryMap[id];
            byte val2 = memoryMap[id ^ 1];
            logChild -= 1; // in first iteration equals log, subsequently reduce 1 from logChild as we traverse up

            if (val1 == logChild && val2 == logChild) {
                memoryMap[parentId] = (byte) (logChild - 1);
            } else {
                byte val = val1 < val2 ? val1 : val2;
                memoryMap[parentId] = val;
            }

            id = parentId;
        }
    }

    private static int log2(int chunkSize) {
        if (chunkSize <= 0) {
            LOGGER.warn("invalid parameter!");
            throw new IllegalArgumentException();
        }
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(chunkSize);
    }

    private void printMemoryMap() {
        int l = 1;
        for (int i = 0; i < this.maxOrder; i++) {
            int j = (int) Math.pow(2, i);
            for (int k = 0; k < j; k++) {
                System.out.print(this.memoryMap[l] + "|");
                l++;
            }
            System.out.println();
        }
        System.out.println();
    }

    public static void main(String[] args) {

        int pageSize = 256;
        int chunkSize = 1024 * 1024 * 64;
        ByteBufferChunk byteBufferChunk = new ByteBufferChunk(pageSize, chunkSize);
        int chunkCount = 8;
        int allocTimes = 102400;
        long start = System.currentTimeMillis();
        for (int i = 0; i < allocTimes; i++) {
//            System.out.println("allocate "+i);
//            long start=System.nanoTime();
            int size = 256;
            ByteBuffer byteBufer = byteBufferChunk.allocateRun(size);
//            System.out.println("alloc "+size+" usage "+(System.nanoTime()-start));
//            start=System.nanoTime();
//            byteBufferArena.recycle(byteBufer);
//            System.out.println("recycle usage "+(System.nanoTime()-start));
        }
        long used = (System.currentTimeMillis() - start);
        System.out.println("total used time  " + used + " avg speed " + allocTimes / used);
    }

    @Override
    public int compareTo(Object o) {
        return -1;
    }
}
