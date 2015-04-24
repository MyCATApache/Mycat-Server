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

public class SimpleConPool {
	private final String url;
	private final String user;
	private final String password;
	private CopyOnWriteArrayList<Connection> cons = new CopyOnWriteArrayList<Connection>();

	public SimpleConPool(String url, String user, String password, int maxCon)
			throws SQLException {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
		for (int i = 0; i < maxCon; i++) {
			cons.add(getCon());
		}
		System.out.println("success ful created connections ,total :" + maxCon);
	}

	public void close() {
		for (Connection con : this.cons) {
			try {
				if (con != null && !con.isClosed()) {
					con.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		cons.clear();
	}

	private Connection getCon() throws SQLException {
		Connection theCon = DriverManager.getConnection(url, user, password);
		return theCon;
	}

	public void returnCon(Connection con) {
		try {
			if (con.isClosed()) {
				System.out.println("closed connection ,aband");
			} else {
				this.cons.add(con);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public Connection getConnection() throws SQLException {
		Connection con = null;
		if (cons.isEmpty()) {
			System.out.println("warn no connection in pool,create new one");
			con = getCon();
			return con;
		} else {
			con = cons.remove(0);
		}
		if (con.isClosed()) {
			System.out.println("warn connection closed ,create new one");
			con = getCon();

		}
		return con;
	}
}