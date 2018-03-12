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
package io.mycat.statistic;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQL统计排序记录器
 *
 * @author mycat
 */
public final class SQLRecorder {

    private final int count;
    SortedSet<SQLRecord> records;

    public SQLRecorder(int count) {
        this.count = count;
        this.records = new ConcurrentSkipListSet<>();
    }

    public List<SQLRecord> getRecords() {
        List<SQLRecord> keyList = new ArrayList<SQLRecord>(records);
        return keyList;
    }


    public void add(SQLRecord record) {
        records.add(record);
    }

    public void clear() {
        records.clear();
    }

    public void recycle(){
        if(records.size() > count){
            SortedSet<SQLRecord> records2 = new ConcurrentSkipListSet<>();
            List<SQLRecord> keyList = new ArrayList<SQLRecord>(records);
            int i = 0;
            for(SQLRecord key : keyList){
                if(i == count) {
                    break;
                }
                records2.add(key);
                i++;
            }
            records = records2;
        }
    }
}