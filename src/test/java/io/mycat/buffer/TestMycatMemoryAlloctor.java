package io.mycat.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zagnix
 * @create 2017-01-18 11:19
 */

public class TestMycatMemoryAlloctor {
    private ConcurrentHashMap<Long,ByteBuf> freeMaps = new ConcurrentHashMap<>();
    final MyCatMemoryAllocator memoryAllocator =
            new MyCatMemoryAllocator(Runtime.getRuntime().availableProcessors()*2);
    @Test
    public void testMemAlloc(){

        for (int i = 0; i <10000/**20000000*/; i++) {
            ByteBuffer byteBuffer = getBuffer(8194);
            byteBuffer.put("helll world".getBytes());
            byteBuffer.flip();
            byte [] src= new byte[byteBuffer.remaining()];
            byteBuffer.get(src);
            Assert.assertEquals("helll world",new String(src));
            free(byteBuffer);
        }
    }


    public ByteBuffer getBuffer(int len)
    {
        ByteBuf byteBuf = memoryAllocator.directBuffer(len);
        ByteBuffer  byteBuffer = byteBuf.nioBuffer(0,len);
        freeMaps.put(PlatformDependent.directBufferAddress(byteBuffer),byteBuf);
        return byteBuffer;
    }

    public void free(ByteBuffer byteBuffer)
    {
        ByteBuf byteBuf1 = freeMaps.get(PlatformDependent.directBufferAddress(byteBuffer));
        byteBuf1.release();
        Assert.assertEquals(0,byteBuf1.refCnt());
    }


    public static String getString(ByteBuffer buffer) {
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try {
            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return "error";
        }
    }

    public static ByteBuffer getByteBuffer(String str)
    {
        return ByteBuffer.wrap(str.getBytes());
    }
}
