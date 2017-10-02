package io.mycat.buffer;

import junit.framework.Assert;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 仿照Netty的思路，针对MyCat内存缓冲策略优化
 * 测试ByteBufferArena
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 17:19 2016/5/17
 * @see @https://github.com/netty/netty
 */
public class TestByteBufferArena {
    int pageSize = 256;
    int chunkSize = 1024 * 8;
    int chunkCount = 8*128;
    @Test
    public void testAllocate() {
        int allocTimes =  1024 ;
        ByteBufferArena byteBufferArena = new ByteBufferArena(chunkSize,pageSize,chunkCount,8);
        long start = System.currentTimeMillis();
        for (int i = 0; i < allocTimes; i++) {
//            System.out.println("allocate "+i);
//            long start=System.nanoTime();
            int size = (i % 1024) + 1 ;
            ByteBuffer byteBufer = byteBufferArena.allocate(size);
            ByteBuffer byteBufer2 = byteBufferArena.allocate(size);
            ByteBuffer byteBufer3 = byteBufferArena.allocate(size);
//            System.out.println("alloc "+size+" usage "+(System.nanoTime()-start));
//            start=System.nanoTime();
            byteBufferArena.recycle(byteBufer);
            byteBufferArena.recycle(byteBufer3);
//            System.out.println("recycle usage "+(System.nanoTime()-start));
        }
        long used = (System.currentTimeMillis() - start);
        System.out.println("ByteBufferArena total used time  " + used + " avg speed " + allocTimes / used);
    }

    @Test
    public void testAllocateDirect() {
        int pageSize = 1024 ;
        int allocTimes = 100;
        DirectByteBufferPool pool = new DirectByteBufferPool(pageSize, (short) 256, (short) 8,0);
        long start = System.currentTimeMillis();
        for (int i = 0; i < allocTimes; i++) {
            //System.out.println("allocate "+i);
            //long start=System.nanoTime();
            int size = (i % 1024) + 1 ;
            ByteBuffer byteBufer = pool.allocate(size);
            ByteBuffer byteBufer2 = pool.allocate(size);
            ByteBuffer byteBufer3 = pool.allocate(size);
            //System.out.println("alloc "+size+" usage "+(System.nanoTime()-start));
            //start=System.nanoTime();
            pool.recycle(byteBufer);
            pool.recycle(byteBufer3);
            //System.out.println("recycle usage "+(System.nanoTime()-start));
        }
        long used = (System.currentTimeMillis() - start);
//        System.out.println("DirectByteBufferPool total used time  " + used + " avg speed " + allocTimes / used);
    }

    @Test
    public void testExpansion(){
        ByteBufferArena byteBufferArena = new ByteBufferArena(1024,8,1,8);
        for (int i = 0; i < 1 ; i++) {
            ByteBuffer byteBufer = byteBufferArena.allocate(256);
            ByteBuffer byteBufer2 = byteBufferArena.allocate(256);
            ByteBuffer byteBufer3 = byteBufferArena.allocate(256);

            byteBufferArena.recycle(byteBufer);
        }
    }

    @Test
    public void testAllocateWithDifferentAddress() {
        int size = 256;
        int pageSize = size * 4;
        int allocTimes = 8;
        ByteBufferArena byteBufferArena = new ByteBufferArena(256*4,256,2,8);
        Map<Long, ByteBuffer> buffs = new HashMap<Long, ByteBuffer>(8);
        ByteBuffer byteBuffer = null;
        DirectBuffer directBuffer = null;
        ByteBuffer temp = null;
        long address;
        boolean failure = false;
        for (int i = 0; i < allocTimes; i++) {
            byteBuffer = byteBufferArena.allocate(size);
            if (byteBuffer == null) {
                Assert.fail("Should have enough memory");
            }
            directBuffer = (DirectBuffer) byteBuffer;
            address = directBuffer.address();
            System.out.println(address);
            temp = buffs.get(address);
            buffs.put(address, byteBuffer);
            if (null != temp) {
                failure = true;
                break;
            }
        }

        for (ByteBuffer buff : buffs.values()) {
            byteBufferArena.recycle(buff);
        }

        if (failure == true) {
            Assert.fail("Allocate with same address");
        }
    }

    @Test
    public void testAllocateNullWhenOutOfMemory() {
        int size = 256;
        int pageSize = size * 4;
        int allocTimes = 9;
        ByteBufferArena pool = new ByteBufferArena(256*4,256,2,8);;
        long start = System.currentTimeMillis();
        ByteBuffer byteBuffer = null;
        List<ByteBuffer> buffs = new ArrayList<ByteBuffer>();
        int i = 0;
        for (; i < allocTimes; i++) {
            byteBuffer = pool.allocate(size);
            if (byteBuffer == null) {
                break;
            }
            buffs.add(byteBuffer);
        }
        for (ByteBuffer buff : buffs) {
            pool.recycle(buff);
        }
    }
}
