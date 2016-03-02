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
package io.mycat.util;

import junit.framework.Assert;

import org.junit.Test;

import io.mycat.util.SplitUtil;

/**
 * @author mycat
 */
public class SplitUtilTest {

    @Test
    public void test() {
        String str = "mysql$1-3,mysql7,mysql9";
        String[] destStr = SplitUtil.split(str, ',', '$', '-');
        Assert.assertEquals(5, destStr.length);
        Assert.assertEquals("mysql1", destStr[0]);
        Assert.assertEquals("mysql2", destStr[1]);
        Assert.assertEquals("mysql3", destStr[2]);
        Assert.assertEquals("mysql7", destStr[3]);
        Assert.assertEquals("mysql9", destStr[4]);
    }

    @Test
    public void test1() {
        String src = "offer$0-3";
        String[] dest = SplitUtil.split(src, '$', true);
        Assert.assertEquals(2, dest.length);
        Assert.assertEquals("offer", dest[0]);
        Assert.assertEquals("0-3", dest[1]);
    }

    @Test
    public void test2() {
        String src = "OFFER_group";
        String[] dest = SplitUtil.split2(src, '$', '-');
        Assert.assertEquals(1, dest.length);
        Assert.assertEquals("OFFER_group", dest[0]);
    }

    @Test
    public void test3() {
        String src = "OFFER_group$2";
        String[] dest = SplitUtil.split2(src, '$', '-');
        Assert.assertEquals(1, dest.length);
        Assert.assertEquals("OFFER_group[2]", dest[0]);
    }

    @Test
    public void test4() {
        String src = "offer$0-3";
        String[] dest = SplitUtil.split2(src, '$', '-');
        Assert.assertEquals(4, dest.length);
        Assert.assertEquals("offer[0]", dest[0]);
        Assert.assertEquals("offer[1]", dest[1]);
        Assert.assertEquals("offer[2]", dest[2]);
        Assert.assertEquals("offer[3]", dest[3]);
    }

}