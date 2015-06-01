package org.opencloudb.route.function;

import junit.framework.Assert;
import org.junit.Test;

/**
 * 跳增一致性哈希分片的测试类
 *
 * @author XiaoSK
 */
public class PartitionByJumpConsistentHashTest {

    @Test
    public void test() {
        int[] expect = {1,2,1,0,0,2,1,1,1,0,2,1,1,2,1,0,0,2,1,0,0,0,2,1};

        PartitionByJumpConsistentHash jch = new PartitionByJumpConsistentHash();
        jch.setTotalBuckets(3);
        jch.init();

        for(int i = 1; i <= expect.length; i++) {
            Assert.assertEquals(true, expect[i-1] == jch.calculate(i + ""));
        }
    }
}
