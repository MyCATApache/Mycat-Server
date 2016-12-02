package io.mycat.sequence;

import io.mycat.MycatServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 全局序列号测试
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:12:05 2016/5/6
 */
public class SequenceTest {
    private Set<String> sequenceSet;
    private long startTime;
    private long endTime;

    @Before
    public void initialize() {
        sequenceSet = new TreeSet<>();
        startTime = System.nanoTime();
    }

//    @Test
//    public void testIncrement(){
//        System.out.print("Increment ");
//        for (int i = 0; i < 1000000; i++) {
//            sequenceSet.add(i+"");
//        }
//    }
//
    @Test
    public void testUUID(){
        System.out.print("UUID ");
        for (int i = 0; i < 100; i++) {
            sequenceSet.add(UUID.randomUUID().toString());
        }
    }

    @Test
    public void testRandom(){
        TreeSet<String> treeSet= new TreeSet<>();
        System.out.println(Long.toBinaryString(Long.valueOf(System.currentTimeMillis()+"")).length());
    }

    @Test
    public void testRandom2(){
        System.out.print("UUID ");
        for (int i = 0; i < 100; i++) {
            sequenceSet.add("aaassscccddd"+i);
        }
    }

    @Test
    public void testXAXID(){
        String xid = MycatServer.getInstance().getXATXIDGLOBAL();
        System.out.println(xid);
    }


    @After
    public void end() {
        endTime = System.nanoTime();
        System.out.println("Time elapsed: " + (endTime - startTime)/(1000000L) + "ms");
    }
}
