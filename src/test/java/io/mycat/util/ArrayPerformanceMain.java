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

import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public class ArrayPerformanceMain {

    public void tArray() {
        byte[] a = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        for (int x = 0; x < 1000000; x++) {
            byte[][] ab = new byte[10][];
            for (int i = 0; i < ab.length; i++) {
                ab[i] = a;
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("array take time:" + (t2 - t1) + " ms.");
    }

    public void tList() {
        byte[] a = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        for (int x = 0; x < 1000000; x++) {
            List<byte[]> ab = new ArrayList<byte[]>(10);
            for (int i = 0; i < ab.size(); i++) {
                ab.add(a);
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("list take time:" + (t2 - t1) + " ms.");
    }

}