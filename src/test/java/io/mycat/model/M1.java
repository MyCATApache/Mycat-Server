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
package io.mycat.model;

import java.util.concurrent.BlockingQueue;

import jsr166y.LinkedTransferQueue;

/**
 * @author mycat
 */
public class M1 {

    private long count;
    private final BlockingQueue<TransferObject> x;
    private final BlockingQueue<TransferObject> y;

    public M1() {
        this.x = new LinkedTransferQueue<TransferObject>();
        this.y = new LinkedTransferQueue<TransferObject>();
    }

    public long getCount() {
        return count;
    }

    public BlockingQueue<TransferObject> getX() {
        return x;
    }

    public BlockingQueue<TransferObject> getY() {
        return y;
    }

    public void start() {
        new Thread(new A(), "A").start();
        new Thread(new B(), "B").start();
        new Thread(new C(), "C").start();
    }

    private final class A implements Runnable {
        @Override
        public void run() {
            for (;;) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                }
                for (int i = 0; i < 1000000; i++) {
                    x.offer(new TransferObject());
                }
            }
        }
    }

    private final class B implements Runnable {
        @Override
        public void run() {
            TransferObject t = null;
            for (;;) {
                try {
                    t = x.take();
                } catch (InterruptedException e) {
                    continue;
                }
                t.handle();
                y.offer(t);
            }
        }
    }

    private final class C implements Runnable {
        @Override
        public void run() {
            TransferObject t = null;
            for (;;) {
                try {
                    t = y.take();
                } catch (InterruptedException e) {
                    continue;
                }
                t.compelete();
                count++;
            }
        }
    }

}