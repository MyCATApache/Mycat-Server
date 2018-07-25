/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.route.function;

import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import org.junit.Assert;
import org.junit.Test;

import static io.mycat.route.function.PartitionByCRC32PreSlot.genDataNodesString;


public class PartitionByCRC32PreSlotTest {

    static TableConfig genTableConfig(int count) {
        RuleConfig rule = new RuleConfig("id", "crc32slot");
        String sb = genDataNodesString(count);
        TableConfig tableConf = new TableConfig("test", "id", true, false, -1, sb,
                null, rule, true, null, false, null, null, null);
        return tableConf;
    }

    @Test
    public void test() {
        PartitionByCRC32PreSlot partition = new PartitionByCRC32PreSlot();
        partition.setRuleName("test");
        partition.setTableConfig(genTableConfig(1000));
        partition.reInit();

        Assert.assertEquals(true, 521 == partition.calculate("1000316"));
        Assert.assertEquals(true, 637 == partition.calculate("2"));


        partition.setTableConfig(genTableConfig(2));
        partition.reInit();

        Assert.assertEquals(true, 0 == partition.calculate("1"));
        Assert.assertEquals(true, 1 == partition.calculate("2"));
        Assert.assertEquals(true, 0 == partition.calculate("3"));
        Assert.assertEquals(true, 1 == partition.calculate("4"));
        Assert.assertEquals(true, 0 == partition.calculate("5"));
        Assert.assertEquals(true, 0 == partition.calculate("6"));
        Assert.assertEquals(true, 0 == partition.calculate("7"));
        Assert.assertEquals(true, 0 == partition.calculate("8"));
        Assert.assertEquals(true, 0 == partition.calculate("9"));

        Assert.assertEquals(true, 0 == partition.calculate("9999"));
        Assert.assertEquals(true, 1 == partition.calculate("123456789"));
        Assert.assertEquals(true, 1 == partition.calculate("35565"));


        partition.setTableConfig(genTableConfig(3));
        partition.reInit();

        Assert.assertEquals(true, 1 == partition.calculate("1"));
        Assert.assertEquals(true, 1 == partition.calculate("2"));
        Assert.assertEquals(true, 0 == partition.calculate("3"));
        Assert.assertEquals(true, 2 == partition.calculate("4"));
        Assert.assertEquals(true, 0 == partition.calculate("5"));
        Assert.assertEquals(true, 1 == partition.calculate("6"));
        Assert.assertEquals(true, 1 == partition.calculate("7"));
        Assert.assertEquals(true, 0 == partition.calculate("8"));
        Assert.assertEquals(true, 0 == partition.calculate("9"));

        Assert.assertEquals(true, 0 == partition.calculate("9999"));
        Assert.assertEquals(true, 2 == partition.calculate("123456789"));
        Assert.assertEquals(true, 2 == partition.calculate("35565"));
    }

    public static void main(String[] args) {

        for (int i = 0; i < 20; i++) {
            int y = 9;
            int count = 3;
            long slot = i % y;
            int slotSize = y / count;

            Long index = slot / slotSize;
            if (slotSize * count != y && index > count - 1) {
                index = index - 1;
            }
            System.out.println(slot + "   " + index);
        }
    }
}
