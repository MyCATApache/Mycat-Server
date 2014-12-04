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
package org.opencloudb;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.util.ExecutorUtil;

/**
 * @author mycat
 */
public class ExecutorTestMain {

    public static void main(String[] args) {
        final AtomicLong count = new AtomicLong(0L);
        final ThreadPoolExecutor executor = ExecutorUtil.create("TestExecutor", 5);

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    long c = count.get();
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("count:" + (count.get() - c) / 5);
                    System.out.println("active:" + executor.getActiveCount());
                    System.out.println("queue:" + executor.getQueue().size());
                    System.out.println("============================");
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            count.incrementAndGet();
                        }
                    });
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            count.incrementAndGet();
                        }
                    });
                }
            }
        }.start();
    }

}