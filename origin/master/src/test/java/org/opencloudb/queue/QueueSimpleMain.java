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

import jsr166y.LinkedTransferQueue;

/**
 * @author mycat
 */
public class QueueSimpleMain {

    static long putCount = 0;
    static long takeCount = 0;

    public static void main(String[] args) {
        // final SynchronousQueue<String> queue = new
        // SynchronousQueue<String>();
        // final ArrayBlockingQueue<String> queue = new
        // ArrayBlockingQueue<String>(10000000);
        final LinkedTransferQueue<String> queue = new LinkedTransferQueue<String>();
        // final LinkedBlockingQueue<String> queue = new
        // LinkedBlockingQueue<String>();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    long put = putCount;
                    long take = takeCount;
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("put:" + (putCount - put) / 5 + " take:" + (takeCount - take) / 5);
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    if (queue.offer("A"))
                        putCount++;
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    // try {
                    if (queue.poll() != null)
                        takeCount++;
                    // } catch (InterruptedException e) {
                    // e.printStackTrace();
                    // }
                    // try {
                    // Thread.sleep(10L);
                    // } catch (InterruptedException e) {
                    // 
                    // e.printStackTrace();
                    // }
                }
            }
        }.start();
    }

}