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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author shenzhw
 * 
 */
public class TestSelectPerf {

	private static AtomicInteger finshiedCount = new AtomicInteger();
	private static AtomicInteger failedCount = new AtomicInteger();

	public static void addFinshed(int count) {
		finshiedCount.addAndGet(count);
	}

	public static void addFailed(int count) {
		failedCount.addAndGet(count);
	}

	private static Connection getCon(String url, String user, String passwd) throws SQLException {
		Connection theCon = DriverManager.getConnection(url, user, passwd);
		return theCon;
	}

	private static void doTest(String url, String user, String password, int threadCount,
			long minId, long maxId, int executetimes, boolean outmidle) {
		final CopyOnWriteArrayList<Thread> threads = new CopyOnWriteArrayList<Thread>();
		final CopyOnWriteArrayList<TravelRecordSelectJob> jobs = new CopyOnWriteArrayList<TravelRecordSelectJob>();
		for (int i = 0; i < threadCount; i++) {
			try {

				Connection con = getCon(url, user, password);
				System.out.println("create thread " + i);
				TravelRecordSelectJob job = new TravelRecordSelectJob(con, minId, maxId,
						executetimes, finshiedCount, failedCount);
				Thread thread = new Thread(job);
				threads.add(thread);
				jobs.add(job);
			} catch (Exception e) {
				System.out.println("failed create thread " + i + " err " + e.toString());
			}
		}
		System.out.println("success create thread count: " + threads.size());
		for (Thread thread : threads) {
			thread.start();
		}
		System.out.println("all thread started,waiting finsh...");
		long start = System.currentTimeMillis();
		boolean notFinished = true;
		int remainThread = 0;
		while (notFinished) {
			notFinished = false;
			remainThread = 0;
			for (Thread thread : threads) {
				if (thread.isAlive()) {
					notFinished = true;
					remainThread++;
				}
			}
			if (remainThread < threads.size() / 2) {
				System.out
						.println("warning many test threads finished ,qps may NOT Accurate ,alive threads:"
								+ remainThread);
			}
			if (outmidle) {
				report(jobs);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		report(jobs);
		System.out.println("finished all,total time :" + (System.currentTimeMillis() - start)
				/ 1000);
	}

	public static void main(String[] args) throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		if (args.length < 5) {
			System.out
					.println("input param,format: [jdbcurl] [user] [password]  [threadpoolsize]  [executetimes] [minId-maxId] [repeat]");
			return;
		}
		int threadCount = 0;// 线程数
		String url = args[0];
		String user = args[1];
		String password = args[2];
		threadCount = Integer.parseInt(args[3]);
		int executetimes = Integer.parseInt(args[4]);
		long minId = Integer.parseInt((args[5].split("-"))[0]);
		long maxId = Integer.parseInt((args[5].split("-"))[1]);
		System.out.println("concerent threads:" + threadCount);
		System.out.println("execute sql times:" + executetimes);
		System.out.println("maxId:" + maxId);
		int repeate = 1;
		if (args.length > 6) {
			repeate = Integer.parseInt(args[6]);
			System.out.println("repeat test times:" + repeate);
		}
		for (int i = 0; i < repeate; i++) {
			try {
				doTest(url, user, password, threadCount, minId, maxId, executetimes, repeate < 2);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static void report(CopyOnWriteArrayList<TravelRecordSelectJob> jobs) {
		int tps = 0;
		for (TravelRecordSelectJob job : jobs) {
			tps += job.getTPS();
		}
		System.out.println("finishend:" + finshiedCount.get() + " failed:" + failedCount.get()
				+ " qps:" + tps);
	}
}