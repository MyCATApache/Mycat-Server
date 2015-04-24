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
package org.opencloudb.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import jsr166y.LinkedTransferQueue;

/**
 * Queue 性能测试
 * 
 * @author mycat
 */
public class QueuePerfMain {

    private static byte[] testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

    private static BlockingQueue<byte[]> arrayQueue = new ArrayBlockingQueue<byte[]>(5000000);
    private static FixedQueue<byte[]> fixedQueue = new FixedQueue<byte[]>(5000000);
    private static Queue<byte[]> testQueue = new Queue<byte[]>();
    private static BlockingQueue<byte[]> linkedQueue = new LinkedBlockingQueue<byte[]>();
    private static LinkedTransferQueue<byte[]> transferQueue = new LinkedTransferQueue<byte[]>();

    public static void tArrayQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    arrayQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += arrayQueue.size();
                    arrayQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tFixedQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    fixedQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += fixedQueue.size();
                    fixedQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    testQueue.append(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += testQueue.size();
                    testQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tLinkedQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    linkedQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += linkedQueue.size();
                    linkedQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tTransferQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    transferQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += transferQueue.size();
                    transferQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void main(String[] args) {
        // testArrayQueue();
        // testFixedQueue();
        // testQueue();
        // testLinkedQueue();
        // testTransferQueue();
    }

}