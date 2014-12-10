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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TravelRecordUpdateJob implements Runnable {
	private final long endId;
	private long finsihed;
	private final int batchSize;
	private final AtomicLong finshiedCount;
	private final AtomicLong failedCount;
	Calendar date = Calendar.getInstance();
	final SimpleConPool conPool;
	DateFormat datafomat = new SimpleDateFormat("yyyy-MM-dd");

	public TravelRecordUpdateJob(SimpleConPool conPool, long totalRecords,
			int batchSize, long startId, AtomicLong finshiedCount,
			AtomicLong failedCount) {
		super();
		this.conPool = conPool;
		this.endId = startId + totalRecords - 1;
		this.batchSize = batchSize;
		this.finsihed = startId;
		this.finshiedCount = finshiedCount;
		this.failedCount = failedCount;
	}

	private int update(Connection con, List<Map<String, String>> list)
			throws SQLException {
		PreparedStatement ps;

		String sql = "update travelrecord set user_id =? ,traveldate=?,fee=?,days=? where id=?";
		ps = con.prepareStatement(sql);
		for (Map<String, String> map : list) {

			ps.setString(1, (String) map.get("user_id"));
			ps.setString(2, (String) map.get("traveldate"));
			ps.setString(3, (String) map.get("fee"));
			ps.setString(4, (String) map.get("days"));
			ps.setLong(5, Long.parseLong(map.get("id")));
			ps.addBatch();
			
		}
		ps.executeBatch();
		return list.size();
	}

	private List<Map<String, String>> getNextBatch() {
		if (finsihed >= endId) {
			return Collections.emptyList();
		}
		long end = (finsihed + batchSize) < this.endId ? (finsihed + batchSize)
				: endId;
		// the last batch
		if (end + batchSize > this.endId) {
			end = this.endId;
		}
		List<Map<String, String>> list = new ArrayList<Map<String, String>>(
				);
		for (long i = finsihed; i <= end; i++) {
			Map<String, String> m = new HashMap<String, String>();
			m.put("id", i + "");
			m.put("user_id", "user " + i);
			m.put("traveldate", getRandomDay(i));
			m.put("fee", i % 10000 + "");
			m.put("days", i % 7 + "");
			list.add(m);
		}
		finsihed += list.size();
		return list;
	}

	private String getRandomDay(long i) {
		int month = Long.valueOf(i % 11 + 1).intValue();
		int day = Long.valueOf(i % 27 + 1).intValue();

		date.set(Calendar.MONTH, month);
		date.set(Calendar.DAY_OF_MONTH, day);
		return datafomat.format(date.getTime());

	}

	@Override
	public void run() {
		Connection con = null;
		try {

			List<Map<String, String>> batch = getNextBatch();
			while (!batch.isEmpty()) {
				try {
					if (con == null || con.isClosed()) {
						con = conPool.getConnection();
						con.setAutoCommit(true);
					}

					update(con, batch);
					finshiedCount.addAndGet(batch.size());
				} catch (Exception e) {
					failedCount.addAndGet(batch.size());
					e.printStackTrace();
				}
				batch = getNextBatch();
			}
		} finally {
			if (con != null) {
				this.conPool.returnCon(con);
			}
		}
	}
}