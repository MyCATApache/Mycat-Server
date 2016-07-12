package io.mycat.memory.unsafe.storage;

import com.google.common.io.Closeables;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by zagnix on 2016/6/4.
 */
public class BlockManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(BlockManagerTest.class);

    @Test
    public  void testNewDiskBlockManager() throws IOException {
        MycatPropertyConf conf = new MycatPropertyConf();
        SerializerManager serializerManager = new SerializerManager();
        DataNodeDiskManager blockManager = new DataNodeDiskManager(conf,true,serializerManager);
        DataNodeFileManager diskBlockManager = blockManager.diskBlockManager();
        /**
         * 生成一个文本文件
         */
        File file = diskBlockManager.getFile("mycat1");
        FileOutputStream fos = new FileOutputStream(file);
        BufferedOutputStream bos = new  BufferedOutputStream(fos);

        bos.write("KOKKKKKK".getBytes());
        bos.flush();
        bos.close();
        fos.close();


        /**
         * 读刚刚写入的文件
         */
        File file1 = diskBlockManager.getFile("mycat1");
        FileInputStream ios = new  FileInputStream(file1);

        BufferedInputStream bin = new BufferedInputStream(ios);
        byte[] str = new byte["KOKKKKKK".getBytes().length];
        int size =  bin.read(str);
        bin.close();
        ios.close();

        Assert.assertEquals("KOKKKKKK",new String(str));



        File file2 = diskBlockManager.getFile("mycat1");

        DiskRowWriter writer = blockManager.
                getDiskWriter(null,file2,DummySerializerInstance.INSTANCE,1024*1024);
        byte [] writeBuffer = new byte[4];
        int v =4;
        writeBuffer[0] = (byte)(v >>> 24);
        writeBuffer[1] = (byte)(v >>> 16);
        writeBuffer[2] = (byte)(v >>>  8);
        writeBuffer[3] = (byte)(v >>>  0);
        writer.write(writeBuffer,0,4);


        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);
        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);
        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);
        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);
        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);
        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);
        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);
        writer.write("you are ok? 1111111111111".getBytes(),0,"you are ok? 1111111111111".getBytes().length);

        writer.close();


        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        assert (file2.length() > 0);
        final BufferedInputStream bs = new BufferedInputStream(new FileInputStream(file2));
        try {
            InputStream in = serializerManager.wrapForCompression(null,bs);
            DataInputStream  din= new DataInputStream(in);
            int numRecords = din.readInt();
            Assert.assertEquals(4,numRecords);
            din.close();
            in.close();
            bs.close();

        } catch (IOException e) {
            Closeables.close(bs, /* swallowIOException = */ true);
            throw e;
        }

    }

    @Test
    public  void testNewDiskBlockWriter(){
        MycatPropertyConf conf = new MycatPropertyConf();
        SerializerManager serializerManager = new SerializerManager();
    }

}
