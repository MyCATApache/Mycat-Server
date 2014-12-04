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
package org.opencloudb.parser.util;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;
import org.opencloudb.parser.util.Pair;
import org.opencloudb.parser.util.PairUtil;

/**
 * @author mycat
 */
public class PairUtilTest extends TestCase {

    @Test
    public void testSequenceSlicing() {
        Assert.assertEquals(new Pair<Integer, Integer>(0, 2), PairUtil.sequenceSlicing("2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 2), PairUtil.sequenceSlicing("1: 2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 0), PairUtil.sequenceSlicing(" 1 :"));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), PairUtil.sequenceSlicing("-1: "));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), PairUtil.sequenceSlicing(" -1:0"));
        Assert.assertEquals(new Pair<Integer, Integer>(0, 0), PairUtil.sequenceSlicing(" :"));
    }

    @Test
    public void splitIndexTest() {
        String src1 = "offer_group[10]";
        Pair<String, Integer> pair1 = PairUtil.splitIndex(src1, '[', ']');
        Assert.assertEquals("offer_group", pair1.getKey());
        Assert.assertEquals(Integer.valueOf(10), pair1.getValue());

        String src2 = "offer_group";
        Pair<String, Integer> pair2 = PairUtil.splitIndex(src2, '[', ']');
        Assert.assertEquals("offer_group", pair2.getKey());
        Assert.assertEquals(Integer.valueOf(-1), pair2.getValue());
    }

}