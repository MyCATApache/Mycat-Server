package io.mycat.memory.unsafe.utils.sort;

import io.mycat.memory.MyCatMemory;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.memory.mm.MemoryManager;
import io.mycat.memory.unsafe.row.BufferHolder;
import io.mycat.memory.unsafe.row.StructType;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.memory.unsafe.row.UnsafeRowWriter;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import io.mycat.sqlengine.mpp.ColMeta;
import io.mycat.sqlengine.mpp.OrderCol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by zagnix on 16-7-9.
 */
public class TestSorter implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TestSorter.class);

    private static  final  int TEST_SIZE = 1000000;
    private static  int TASK_SIZE = 100;
    private static  CountDownLatch countDownLatch = new CountDownLatch(100);
    public  void  runSorter( MyCatMemory myCatMemory,
                             MemoryManager memoryManager,
                             MycatPropertyConf conf) throws NoSuchFieldException, IllegalAccessException, IOException {
        DataNodeMemoryManager dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager,
                Thread.currentThread().getId());
        /**
         * 1.schema ,模拟一个field字段值
         *
         */
        int fieldCount = 3;
        ColMeta colMeta = null;
        Map<String, ColMeta> colMetaMap = new HashMap<String, ColMeta>(fieldCount);
        colMeta = new ColMeta(0, ColMeta.COL_TYPE_STRING);
        colMetaMap.put("id", colMeta);
        colMeta = new ColMeta(1, ColMeta.COL_TYPE_STRING);
        colMetaMap.put("name", colMeta);
        colMeta = new ColMeta(2, ColMeta.COL_TYPE_STRING);
        colMetaMap.put("age", colMeta);


        OrderCol[] orderCols = new OrderCol[1];
        OrderCol orderCol = new OrderCol(colMetaMap.get("id"),
                OrderCol.COL_ORDER_TYPE_ASC);
        orderCols[0] = orderCol;
        /**
         * 2 .PrefixComputer
         */
        StructType schema = new StructType(colMetaMap, fieldCount);
        schema.setOrderCols(orderCols);

        UnsafeExternalRowSorter.PrefixComputer prefixComputer =
                new RowPrefixComputer(schema);

        /**
         * 3 .PrefixComparator 默认是ASC，可以选择DESC
         */
        final PrefixComparator prefixComparator = PrefixComparators.LONG;

        UnsafeExternalRowSorter sorter =
                new UnsafeExternalRowSorter(dataNodeMemoryManager,
                        myCatMemory,
                        schema,
                        prefixComparator,
                        prefixComputer,
                        conf.getSizeAsBytes("mycat.buffer.pageSize","1m"),
                        true, /**使用基数排序？true or false*/
                        true);
        UnsafeRow unsafeRow;
        BufferHolder bufferHolder;
        UnsafeRowWriter unsafeRowWriter;
        String line = "testUnsafeRow";
        final Random rand = new Random(42);
        for (int i = 0; i < TEST_SIZE; i++) {
            unsafeRow = new UnsafeRow(3);
            bufferHolder = new BufferHolder(unsafeRow);
            unsafeRowWriter = new UnsafeRowWriter(bufferHolder,3);
            bufferHolder.reset();

            String key = getRandomString(rand.nextInt(300)+100);

            unsafeRowWriter.write(0,key.getBytes());
            unsafeRowWriter.write(1, line.getBytes());
            unsafeRowWriter.write(2, ("35" + 1).getBytes());

            unsafeRow.setTotalSize(bufferHolder.totalSize());
            sorter.insertRow(unsafeRow);
        }
        Iterator<UnsafeRow> iter = sorter.sort();
        UnsafeRow row = null;
        int indexprint = 0;
        while (iter.hasNext()) {
            row = iter.next();
            indexprint++;
        }

        sorter.cleanupResources();
        countDownLatch.countDown();

        System.out.println("Thread ID :" + Thread.currentThread().getId() + "Index : " + indexprint);
    }


    public static String getRandomString(int length) { //length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }
    final  MyCatMemory myCatMemory ;
    final  MemoryManager memoryManager;
    final  MycatPropertyConf conf;


        public TestSorter( MyCatMemory myCatMemory, MemoryManager memoryManager,MycatPropertyConf conf) throws NoSuchFieldException, IllegalAccessException {
            this.myCatMemory = myCatMemory;
            this.memoryManager = memoryManager;
            this.conf = conf;
        }

        @Override
        public void run() {
            try {
                runSorter(myCatMemory,memoryManager,conf);
            } catch (NoSuchFieldException e) {
                logger.error(e.getMessage());
            } catch (IllegalAccessException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

    public static void main(String[] args) throws Exception {

        MyCatMemory myCatMemory ;
        MemoryManager memoryManager;
        MycatPropertyConf conf;

        myCatMemory = new MyCatMemory();
        memoryManager = myCatMemory.getResultMergeMemoryManager();
        conf = myCatMemory.getConf();

        for (int i = 0; i < TASK_SIZE; i++) {
            Thread thread = new Thread(new TestSorter(myCatMemory,memoryManager,conf));
            thread.start();
        }

        while (countDownLatch.getCount() != 0){
            System.err.println("count ========================>" + countDownLatch.getCount());
            Thread.sleep(1000);
        }

        System.err.println(TASK_SIZE + " tasks sorter finished ok !!!!!!!!!");

        System.exit(1);
    }

}
