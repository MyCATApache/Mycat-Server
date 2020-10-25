
package io.mycat.route.function;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByLongTest {

    @Test
    public void testCalculate1() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("4");
        rule.setPartitionLength("256");
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "256";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "512";
        Assert.assertEquals(true, 2 == rule.calculate(value));
        value = "768";
        Assert.assertEquals(true, 3 == rule.calculate(value));
        value = "1023";
        Assert.assertEquals(true, 3 == rule.calculate(value));
    }

    @Test
    public void testCalculate2() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("256");
        rule.setPartitionLength("4");
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "5";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "10";
        Assert.assertEquals(true, 2 == rule.calculate(value));
        value = "768";
        Assert.assertEquals(true, 192 == rule.calculate(value));
        value = "1023";
        Assert.assertEquals(true, 255 == rule.calculate(value));
    }


}
