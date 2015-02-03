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
package org.opencloudb.performance;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class UserTableSelectJob implements Runnable, SelectJob {
	private final Connection con;

	private final int executeTimes;
	Random random = new Random();
	private final AtomicInteger finshiedCount;
	private final AtomicInteger failedCount;
	private volatile long usedTime;
	private volatile long success;
	private volatile long maxTTL = 0;
	private volatile long minTTL = Integer.MAX_VALUE;
	private volatile long validTTLSum = 0;
	private volatile long validTTLCount = 0;
	private LinkedList<StringItem> sqlTemplateItems;

	public UserTableSelectJob(Connection con,
			LinkedList<StringItem> sqlTemplateItems, int executeTimes,
			AtomicInteger finshiedCount, AtomicInteger failedCount) {
		super();
		this.con = con;
		this.sqlTemplateItems = sqlTemplateItems;
		this.executeTimes = executeTimes;
		this.finshiedCount = finshiedCount;
		this.failedCount = failedCount;
	}

	private long select() {
		ResultSet rs = null;
		long used = -1;

		try {
			String sql = RandomDataValueUtil
					.evalRandValueString(sqlTemplateItems);
			long startTime = System.currentTimeMillis();
			rs = con.createStatement().executeQuery(sql);
			if (rs.next()) {
			}
			used = System.currentTimeMillis() - startTime;
			finshiedCount.addAndGet(1);
			success++;
		} catch (Exception e) {
			failedCount.addAndGet(1);
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		}
		return used;
	}

	@Override
	public void run() {
		long curmaxTTL = this.maxTTL;
		long curminTTL = this.minTTL;
		long curvalidTTLSum = this.validTTLSum;
		long curvalidTTLCount = this.validTTLCount;

		long start = System.currentTimeMillis();
		for (int i = 0; i < executeTimes; i++) {

			long ttlTime = this.select();
			if (ttlTime != -1) {
				if (ttlTime > curmaxTTL) {
					curmaxTTL = ttlTime;
				} else if (ttlTime < curminTTL) {
					curminTTL = ttlTime;
				}
				curvalidTTLSum += ttlTime;
				curvalidTTLCount += 1;
			}
			usedTime = System.currentTimeMillis() - start;
			if (i % 100 == 0) {
				maxTTL = curmaxTTL;
				minTTL = curminTTL;
				validTTLSum = curvalidTTLSum;
				validTTLCount = curvalidTTLCount;
			}
		}
		maxTTL = curmaxTTL;
		minTTL = curminTTL;
		validTTLSum = curvalidTTLSum;
		validTTLCount = curvalidTTLCount;
		try {
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public long getUsedTime() {
		return this.usedTime;
	}

	public double getTPS() {
		if (usedTime > 0) {
			return  (this.success * 1000+0.0) / this.usedTime;
		} else {
			return 0;
		}
	}

	public long getMaxTTL() {
		return maxTTL;
	}

	public long getMinTTL() {
		return minTTL;
	}

	public long getValidTTLSum() {
		return validTTLSum;
	}

	public long getValidTTLCount() {
		return validTTLCount;
	}

	public static void main(String[] args) {
		Random r = new Random();
		for (int i = 0; i < 10; i++) {
			int f = r.nextInt(90000 - 80000) + 80000;
			System.out.println(f);
		}
	}
}