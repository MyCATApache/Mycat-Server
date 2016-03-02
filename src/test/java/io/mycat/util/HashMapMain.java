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

import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat
 */
public class HashMapMain {

    public void t() {
        String[] keys = new String[] { "a", "b", "c", "d", "e" };
        long t = System.currentTimeMillis();
        int count = 1000000;
        Map<String, String> m = new HashMap<String, String>();
        t = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            for (String key : keys) {
                m.put(key, "String.value");
            }
            for (String key : keys) {
                m.remove(key);
            }
        }
        System.out.println((System.currentTimeMillis() - t) * 1000 * 1000 / (count * keys.length * 2) + " ns");
    }

}