package io.mycat.memory.unsafe.storage;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by zagnix on 2016/6/4.
 */
public class SerializerManagerTest {
    @Test
    public  void testNewSerializerManager() throws IOException {
        SerializerManager serializerManager = new SerializerManager();
        final int[] value = new int[1];
        OutputStream s = serializerManager.wrapForCompression(null, new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                     value[0] = b;
            }
        });

        s.write(10);
        Assert.assertEquals(10,value[0]);

        InputStream in = serializerManager.wrapForCompression(null, new InputStream() {
            @Override
            public int read() throws IOException {
                return 10;
            }
        });
        Assert.assertEquals(10,in.read());
    }
}
