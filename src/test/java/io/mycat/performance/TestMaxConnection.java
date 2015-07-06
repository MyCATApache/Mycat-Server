/* Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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

import java.sql.SQLException;

public class TestMaxConnection {
	public static void main(String[] args) {
		if (args.length < 4) {
			System.out
					.println("input param,format: [jdbcurl] [user] [password]  [poolsize] ");
			return;
		}
		String url = args[0];
		String user = args[1];
		String password = args[2];
		Integer poolsize = Integer.parseInt(args[3]);
		SimpleConPool pool = null;
        long start=System.currentTimeMillis();
		try {
			pool = new SimpleConPool(url, user, password, poolsize);

		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("success create threadpool ,used time "+(System.currentTimeMillis()-start));
		int i = 0;
		try {
			for (i = 0; i < poolsize; i++) {
				pool.getConnection().createStatement()
						.execute("select * from company limit 1");
			}
		} catch (SQLException e) {
			System.out.println("exectute  sql err " + i + " err:"
					+ e.toString());
		} finally {
			pool.close();
		}
	}
}
