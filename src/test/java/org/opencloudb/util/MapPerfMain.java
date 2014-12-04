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
package org.opencloudb.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;

/**
 * @author mycat
 */
public class MapPerfMain {

    public void t1() {
        Map<String, Date> m = new HashMap<String, Date>();
        for (int i = 0; i < 100000; i++) {
            m.put(UUID.randomUUID().toString(), new Date());
        }
        remove1(m);
        Assert.assertEquals(0, m.size());
    }

    public void t2() {
        Map<String, Date> m = new HashMap<String, Date>();
        for (int i = 0; i < 100000; i++) {
            m.put(UUID.randomUUID().toString(), new Date());
        }
        remove2(m);
        Assert.assertEquals(0, m.size());
    }

    void remove1(Map<String, Date> m) {
        Iterator<Map.Entry<String, Date>> it = m.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue();
            it.remove();
        }
    }

    void remove2(Map<String, Date> m) {
        Iterator<Map.Entry<String, Date>> it = m.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue();
        }
        m.clear();
    }

}