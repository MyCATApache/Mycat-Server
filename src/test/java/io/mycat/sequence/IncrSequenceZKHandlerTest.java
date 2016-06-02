package io.mycat.sequence;

import io.mycat.route.sequence.handler.IncrSequenceZKHandler;
import io.mycat.route.util.PropertiesUtil;
import junit.framework.Assert;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * zookeeper 实现递增序列号
 * 默认测试模拟60个进程，每个进程内20个线程。每个线程调用50次参数为GLOBAL的nextid
 * 默认GLOBAL.MINID=1
 * 默认GLOBAL.MAXID=10
 * 表示当前线程内id用光时，每次会取GLOBAL.MINID-GLOBAL.MAXID9个ID
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 23:35 2016/5/6
 */
public class IncrSequenceZKHandlerTest {
    private final static int MAX_CONNECTION = 5;
    private final static int threadCount = 5;
    private final static int LOOP = 5;
    TestingServer testingServer = null;
    IncrSequenceZKHandler incrSequenceZKHandler[];
    ConcurrentSkipListSet<Long> results;

    @Before
    public void initialize() throws Exception {
        testingServer = new TestingServer();
        testingServer.start();
        incrSequenceZKHandler = new IncrSequenceZKHandler[MAX_CONNECTION];
        results = new ConcurrentSkipListSet();
    }

    @Test
    public void testCorrectnessAndEfficiency() throws InterruptedException {
        final Thread threads[] = new Thread[MAX_CONNECTION];
        for (int i = 0; i < MAX_CONNECTION; i++) {
            final int a = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    incrSequenceZKHandler[a] = new IncrSequenceZKHandler();
                    Properties props = PropertiesUtil.loadProps("sequence_conf.properties");
                    try {
                        incrSequenceZKHandler[a].initializeZK(props, testingServer.getConnectString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Thread threads[] = new Thread[threadCount];
                    for (int j = 0; j < threadCount; j++) {
                        threads[j] = new Thread() {
                            @Override
                            public void run() {
                                for (int k = 0; k < LOOP; k++) {
                                    long key = incrSequenceZKHandler[a].nextId("GLOBAL");
                                    results.add(key);
                                }
                            }
                        };
                        threads[j].start();
                    }
                    for (int j = 0; j < threadCount; j++) {
                        try {
                            threads[j].join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };

        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < MAX_CONNECTION; i++) {
            threads[i].start();
        }
        for (int i = 0; i < MAX_CONNECTION; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        Assert.assertEquals(MAX_CONNECTION * LOOP * threadCount, results.size());
//        Assert.assertTrue(results.pollLast().equals(MAX_CONNECTION * LOOP * threadCount + 1L));
//        Assert.assertTrue(results.pollFirst().equals(2L));
        System.out.println("Time elapsed:" + ((double) (end - start + 1) / 1000.0) + "s\n TPS:" + ((double) (MAX_CONNECTION * LOOP * threadCount) / (double) (end - start + 1) * 1000.0) + "/s");
    }
}
