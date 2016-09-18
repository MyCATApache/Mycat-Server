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

/**
 * @author mycat
 */
public class M1Main {

    public static void main(String[] args) {
        final M1 m1 = new M1();
        m1.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    long c = m1.getCount();
                    try {
                        Thread.sleep(2000L);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    System.out.println("tps:" + (m1.getCount() - c) / 2);
                    System.out.println("  x:" + m1.getX().size());
                    System.out.println("  y:" + m1.getY().size());
                    System.out.println("==============");
                }
            }
        }.start();
    }

}