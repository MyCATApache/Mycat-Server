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

import java.util.Collection;
import java.util.Iterator;

import io.mycat.util.SmallSet;
import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author mycat
 */
public class SmallSetTest extends TestCase {

    public void assertListEquals(Collection<? extends Object> col, Object... objects) {
        if (objects == null) {
            Assert.assertTrue(col.isEmpty());
        }
        Assert.assertEquals(objects.length, col.size());
        int i = 0;
        for (Object o : col) {
            Assert.assertEquals(objects[i++], o);
        }
    }

    public void testSet() throws Exception {
        SmallSet<Object> sut = new SmallSet<Object>();
        sut.add(1);
        Assert.assertEquals(1, sut.size());
        Iterator<Object> iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertFalse(iter.hasNext());
        assertListEquals(sut, 1);
        try {
            iter.next();
            Assert.assertTrue(false);
        } catch (Exception e) {
        }

        sut = new SmallSet<Object>();
        sut.add(1);
        Assert.assertEquals(1, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertFalse(iter.hasNext());
        assertListEquals(sut, 1);
        iter.remove();
        Assert.assertEquals(0, sut.size());
        Assert.assertFalse(iter.hasNext());
        iter = sut.iterator();
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertFalse(iter.hasNext());
        assertListEquals(sut, 1, 2);
        iter.remove();
        assertListEquals(sut, 1);
        Assert.assertEquals(1, sut.size());
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        assertListEquals(sut, 1, 2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertEquals(1, sut.size());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        assertListEquals(sut, 1, 2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertEquals(1, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        assertListEquals(sut, 1, 2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertEquals(1, sut.size());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        iter.remove();
        assertListEquals(sut);
        iter = sut.iterator();
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        sut.add(3);
        assertListEquals(sut, 1, 2, 3);
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        assertListEquals(sut, 1, 2, 3);
        iter.remove();
        assertListEquals(sut, 2, 3);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        iter.remove();
        assertListEquals(sut, 3);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(3, iter.next());
        Assert.assertFalse(iter.hasNext());
        iter.remove();
        assertListEquals(sut);
        Assert.assertFalse(iter.hasNext());
        iter = sut.iterator();
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        sut.add(3);
        assertListEquals(sut, 1, 2, 3);
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        assertListEquals(sut, 1, 2, 3);
        iter.remove();
        assertListEquals(sut, 2, 3);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(3, iter.next());
        Assert.assertFalse(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertFalse(iter.hasNext());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
    }

}