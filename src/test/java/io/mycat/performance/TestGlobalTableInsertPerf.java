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
package io.mycat.performance;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author shenzhw
 * 
 */
public class TestGlobalTableInsertPerf  extends AbstractMultiTreadBatchTester{
	public boolean  parseArgs(String[] args) {
		if (args.length < 5) {
			System.out
					.println("input param,format: [jdbcurl] [user] [password]  [threadpoolsize]  recordcount ");
			return false;
		}
		url = args[0];
		user = args[1];
		password = args[2];
		threadCount = Integer.parseInt(args[3]);
		rangeItems = new String[]{"0-"+Integer.parseInt(args[4])};
		return true;

	}
	public static void main(String[] args) throws Exception {
	       new TestGlobalTableInsertPerf().run(args);
	       

		}
	@Override
	public Runnable createJob(SimpleConPool conPool2, long myCount,
			int batch, long startId, AtomicLong finshiedCount2,
			AtomicLong failedCount2) {
		 return new GoodsInsertJob(conPool2,
					myCount, batch, startId, finshiedCount, failedCount);
	}

	

	
	
}