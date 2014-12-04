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
package org.opencloudb.route.util;

import org.junit.Assert;
import org.junit.Test;
import org.opencloudb.route.util.PartitionUtil;

/**
 * @author mycat()
 */
public class PartitionUtilTest {

    @Test
    public void testPartition() {
        // 本例的分区策略：希望将数据水平分成3份，前两份各占25%，第三份占50%。（故本例非均匀分区）
        // |<---------------------1024------------------------>|
        // |<----256--->|<----256--->|<----------512---------->|
        // | partition0 | partition1 | partition2 |
        // | 共2份,故count[0]=2 | 共1份，故count[1]=1 |
        int[] count = new int[] { 2, 1 };
        int[] length = new int[] { 256, 512 };
        PartitionUtil pu = new PartitionUtil(count, length);

        // 下面代码演示分别以offerId字段或memberId字段根据上述分区策略拆分的分配结果
        int DEFAULT_STR_HEAD_LEN = 8; // cobar默认会配置为此值
        long offerId = 12345;
        String memberId = "qiushuo";

        // 若根据offerId分配，partNo1将等于0，即按照上述分区策略，offerId为12345时将会被分配到partition0中
        int partNo1 = pu.partition(offerId);

        // 若根据memberId分配，partNo2将等于2，即按照上述分区策略，memberId为qiushuo时将会被分到partition2中
        int partNo2 = pu.partition(memberId, 0, DEFAULT_STR_HEAD_LEN);

        Assert.assertEquals(0, partNo1);
        Assert.assertEquals(2, partNo2);
    }

}