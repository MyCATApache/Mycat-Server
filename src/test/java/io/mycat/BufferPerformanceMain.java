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
package io.mycat;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public class BufferPerformanceMain {

    public void getAllocate() {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Get:allocate)");
    }

    public void getAllocateDirect() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Get:allocateDirect)");
    }

    public void putAllocate() {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.put(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Put:allocate)");
    }

    public void putAllocateDirect() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.put(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Put:allocateDirect)");
    }

    public void copyArrayDirect() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        while (buffer.hasRemaining()) {
            buffer.put((byte) 1);
        }
        byte[] b = new byte[1024];
        int count = 10000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b, 0, b.length);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(testCopyArrayDirect)");
    }

    public void copyArray() {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        while (buffer.hasRemaining()) {
            buffer.put((byte) 1);
        }
        byte[] b = new byte[1024];
        int count = 10000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b, 0, b.length);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(testCopyArray)");
    }

    public static void main(String[] args) {

    }

}