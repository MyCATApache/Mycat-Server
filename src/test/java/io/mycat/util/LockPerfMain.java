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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class LockPerfMain {

    public void tReentrantLock() {
        System.currentTimeMillis();
        ReentrantLock lock = new ReentrantLock();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            if (lock.tryLock()) {
                try {
                    // ...
                } finally {
                    lock.unlock();
                }
            }
        }
        long t2 = System.currentTimeMillis();

        System.out.println("take time:" + (t2 - t1) + " ms.");
    }

    public void tAtomicBoolean() {
        System.currentTimeMillis();
        AtomicBoolean atomic = new AtomicBoolean();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            if (atomic.compareAndSet(false, true)) {
                try {
                    // ...
                } finally {
                    atomic.set(false);
                }
            }
        }
        long t2 = System.currentTimeMillis();

        System.out.println("take time:" + (t2 - t1) + " ms.");
    }

}