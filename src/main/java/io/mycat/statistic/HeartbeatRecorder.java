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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.util.TimeUtil;

/**
 * 记录最近3个时段的平均响应时间，默认1，10，30分钟。
 * 
 * @author mycat
 */
public class HeartbeatRecorder {

    private static final int MAX_RECORD_SIZE = 256;
    private static final long AVG1_TIME = 60 * 1000L;
    private static final long AVG2_TIME = 10 * 60 * 1000L;
    private static final long AVG3_TIME = 30 * 60 * 1000L;
    private static final long SWAP_TIME = 24 * 60 * 60 * 1000L;

    private long avg1;
    private long avg2;
    private long avg3;
    private final Queue<Record> records;
    private final Queue<Record> recordsAll;
    
	private static final Logger LOGGER = LoggerFactory.getLogger("DataSourceSyncRecorder");

    public HeartbeatRecorder() {
        this.records = new ConcurrentLinkedQueue<Record>();
        this.recordsAll = new ConcurrentLinkedQueue<Record>();
    }

    public String get() {
        return new StringBuilder().append(avg1).append(',').append(avg2).append(',').append(avg3).toString();
    }

    public void set(long value) {
    	try{
    		long time = TimeUtil.currentTimeMillis();
            if (value < 0) {
                recordsAll.offer(new Record(0, time));
                return;
            }
            remove(time);
            int size = records.size();
            if (size == 0) {
                records.offer(new Record(value, time));
                avg1 = avg2 = avg3 = value;
                return;
            }
            if (size >= MAX_RECORD_SIZE) {
                records.poll();
            }
            records.offer(new Record(value, time));
            recordsAll.offer(new Record(value, time));
            calculate(time);
    	}catch(Exception e){ 
    		LOGGER.error("record HeartbeatRecorder error " ,e);
    	}
    }

    /**
     * 删除超过统计时间段的数据
     */
    private void remove(long time) {
        final Queue<Record> records = this.records;
        while (records.size() > 0) {
            Record record = records.peek();
            if (time >= record.time + AVG3_TIME) {
                records.poll();
            } else {
                break;
            }
        }
        
        final Queue<Record> recordsAll = this.recordsAll;
        while (recordsAll.size() > 0) {
            Record record = recordsAll.peek();
            if (time >= record.time + SWAP_TIME) {
            	recordsAll.poll();
            } else {
                break;
            }
        }
    }

    /**
     * 计算记录的统计数据
     */
    private void calculate(long time) {
        long v1 = 0L, v2 = 0L, v3 = 0L;
        int c1 = 0, c2 = 0, c3 = 0;
        for (Record record : records) {
            long t = time - record.time;
            if (t <= AVG1_TIME) {
                v1 += record.value;
                ++c1;
            }
            if (t <= AVG2_TIME) {
                v2 += record.value;
                ++c2;
            }
            if (t <= AVG3_TIME) {
                v3 += record.value;
                ++c3;
            }
        }
        avg1 = (v1 / c1);
        avg2 = (v2 / c2);
        avg3 = (v3 / c3);
    }

    public Queue<Record> getRecordsAll() {
		return this.recordsAll;
	}

	/**
     * @author mycat
     */
    public static class Record {
    	private long value;
    	private long time;

        Record(long value, long time) {
            this.value = value;
            this.time = time;
        }
		public long getValue() {
			return this.value;
		}
		public void setValue(long value) {
			this.value = value;
		}
		public long getTime() {
			return this.time;
		}
		public void setTime(long time) {
			this.time = time;
		}
        
        
    }
}
