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
package org.opencloudb.config.model;

import java.util.Map;

import org.opencloudb.backend.PhysicalDBPool;

/**
 * Datahost is a group of DB servers which is synchronized with each other
 * 
 * @author wuzhih
 * 
 */
public class DataHostConfig {
	private String name;
	private int maxCon = SystemConfig.DEFAULT_POOL_SIZE;
	private int minCon = 10;
	private int balance = PhysicalDBPool.BALANCE_NONE;
	private int writeType = PhysicalDBPool.WRITE_ONLYONE_NODE;
	private final String dbType;
	private final String dbDriver;
	private final DBHostConfig[] writeHosts;
	private final Map<Integer, DBHostConfig[]> readHosts;
	private String hearbeatSQL;
	private String connectionInitSql;

	public DataHostConfig(String name, String dbType, String dbDriver,
			DBHostConfig[] writeHosts, Map<Integer, DBHostConfig[]> readHosts) {
		super();
		this.name = name;
		this.dbType = dbType;
		this.dbDriver = dbDriver;
		this.writeHosts = writeHosts;
		this.readHosts = readHosts;
	}

	public String getConnectionInitSql()
	{
		return connectionInitSql;
	}

	public void setConnectionInitSql(String connectionInitSql)
	{
		this.connectionInitSql = connectionInitSql;
	}

	public int getWriteType() {
		return writeType;
	}

	public void setWriteType(int writeType) {
		this.writeType = writeType;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getMaxCon() {
		return maxCon;
	}

	public void setMaxCon(int maxCon) {
		this.maxCon = maxCon;
	}

	public int getMinCon() {
		return minCon;
	}

	public void setMinCon(int minCon) {
		this.minCon = minCon;
	}

	public int getBalance() {
		return balance;
	}

	public void setBalance(int balance) {
		this.balance = balance;
	}

	public String getDbType() {
		return dbType;
	}

	public String getDbDriver() {
		return dbDriver;
	}

	public DBHostConfig[] getWriteHosts() {
		return writeHosts;
	}

	public Map<Integer, DBHostConfig[]> getReadHosts() {
		return readHosts;
	}

	public String getHearbeatSQL() {
		return hearbeatSQL;
	}

	public void setHearbeatSQL(String heartbeatSQL) {
		this.hearbeatSQL = heartbeatSQL;

	}

}