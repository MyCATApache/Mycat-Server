package io.mycat.sequence;

import io.mycat.config.MycatConfig;
import io.mycat.route.sequence.handler.DistributedSequenceHandler;
import junit.framework.Assert;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于ZK与本地配置的分布式ID生成器
 * 无悲观锁，吞吐量更高
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:12:05 2016/5/3
 */
public class DistributedSequenceHandlerTest {
    TestingServer testingServer = null;
    DistributedSequenceHandler distributedSequenceHandler[];

    @Before
    public void initialize() throws Exception {
        distributedSequenceHandler = new DistributedSequenceHandler[16];
        MycatConfig mycatConfig = new MycatConfig();
        testingServer = new TestingServer();
        testingServer.start();
        for (int i = 0; i < 16; i++) {
            distributedSequenceHandler[i] = new DistributedSequenceHandler(mycatConfig.getSystem());
            distributedSequenceHandler[i].initializeZK(testingServer.getConnectString());
            distributedSequenceHandler[i].nextId("");
        }
    }

    /**
     * 测试获取的唯一InstanceId
     *
     * @throws Exception
     */
    @Test
    public void testUniqueInstanceID() throws Exception {
        Set<Long> idSet = new HashSet<>();
        for (int i = 0; i < 16; i++) {
            idSet.add(distributedSequenceHandler[i].getInstanceId());
        }
        Assert.assertEquals(idSet.size(), 16);
    }

    /**
     * 测试获取的唯一id
     *
     * @throws Exception
     */
    @Test
    public void testUniqueID() throws Exception {
        final ConcurrentHashMap<Long, String> idSet = new ConcurrentHashMap<>();
        Thread thread[] = new Thread[10];
        long start = System.currentTimeMillis();
        //多少线程，注意线程数不能超过最大线程数（1<<threadBits）
        for (int i = 0; i < 10; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 100; j++) {
                        for (int i = 0; i < 16; i++) {
                            idSet.put(distributedSequenceHandler[i].nextId(""), "");
                        }
                    }

                }
            };
            thread[i].start();
        }
        for (int i = 0; i < 10; i++) {
            thread[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println("Time elapsed:" + (double) (end - start) / 1000.0 + "s");
        System.out.println("ID/s:" + (((double) idSet.size()) / ((double) (end - start) / 1000.0)));
        Assert.assertEquals(idSet.size(), 16000);
    }

    /**
     * 测试ZK容灾
     *
     * @throws Exception
     */
    @Test
    public void testFailOver() {
        Set<Long> idSet = new HashSet<>();
        try {
            int leader = failLeader(17);
            System.out.println("***断掉一个leader节点后（curator会抛对应的异常断链异常，不用在意）***：");
            for (int i = 0; i < 16; i++) {
                if (i == leader) {
                    System.out.println("Node [" + i + "] used to be leader");
                    continue;
                }
                distributedSequenceHandler[i].nextId("");
                System.out.println("Node [" + i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership() );
                System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
                idSet.add(distributedSequenceHandler[i].getInstanceId());
            }
            Assert.assertEquals(idSet.size(), 15);
            idSet = new HashSet<>();
            int leader2 = failLeader(leader);
            System.out.println("***断掉两个leader节点后（curator会抛对应的异常断链异常，不用在意）***：");
            for (int i = 0; i < 16; i++) {
                if (i == leader || i == leader2) {
                    System.out.println("Node ["+i + " used to be leader");
                    continue;
                }
                distributedSequenceHandler[i].nextId("");
                System.out.println("Node ["+i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership());
                System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
                idSet.add(distributedSequenceHandler[i].getInstanceId());
            }
            Assert.assertEquals(idSet.size(), 14);

            idSet = new HashSet<>();
            MycatConfig mycatConfig = new MycatConfig();
            distributedSequenceHandler[leader] = new DistributedSequenceHandler(mycatConfig.getSystem());
            distributedSequenceHandler[leader].initializeZK(testingServer.getConnectString());
            distributedSequenceHandler[leader].nextId("");
            distributedSequenceHandler[leader2] = new DistributedSequenceHandler(mycatConfig.getSystem());
            distributedSequenceHandler[leader2].initializeZK(testingServer.getConnectString());
            distributedSequenceHandler[leader2].nextId("");
            System.out.println("新加入两个节点后");
            for (int i = 0; i < 16; i++) {
                System.out.println("Node ["+i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership() );
                System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
                idSet.add(distributedSequenceHandler[i].getInstanceId());
            }
        } catch (Exception e) {

        } finally {
            Assert.assertEquals(idSet.size(), 16);
        }

    }

    private int failLeader(int p) {
        int leader = 0, follower = 0;
        for (int i = 0; i < 16; i++) {
            if (i == p) continue;
            if (distributedSequenceHandler[i].getLeaderSelector().hasLeadership()) {
                leader = i;
            } else {
                follower = i;
            }
            System.out.println("Node ["+i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership() );
            System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
        }
        try {
            distributedSequenceHandler[leader].close();
        } catch (IOException e) {
        }

        while (true) {
            follower++;
            if (follower >= 16) follower = 0;
            if (follower == leader || follower == p) continue;
            if (distributedSequenceHandler[follower].getLeaderSelector().hasLeadership()) {
                break;
            }
        }
        return leader;
    }

}
