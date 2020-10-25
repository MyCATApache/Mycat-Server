
package io.mycat.route.function;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByModTest {

    @Test
    public void testCalculate1() {
        PartitionByMod rule = new PartitionByMod();
        rule.setCount(256);
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "257";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "200";
        Assert.assertEquals(true, 200 == rule.calculate(value));
        value = "201";
        Assert.assertEquals(true, 201 == rule.calculate(value));
    }



}
