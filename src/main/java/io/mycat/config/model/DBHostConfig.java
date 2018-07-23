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
package io.mycat.config.model;

/**
 * 数据库主机配置
 */
public class DBHostConfig {

	// 连接池中连接空闲超时时间
	private long idleTimeout = SystemConfig.DEFAULT_IDLE_TIMEOUT;
	// 主机名称
	private final String hostName;
	// IP
	private final String ip;
	// 端口
	private final int port;
	private final String url;
	// 用户
	private final String user;
	// 密码
	private final String password;
	// 密文
    private final String encryptPassword;
    // ssl
    private final boolean useSSL;
    // 最大连接数
	private int maxCon ;
	// 最小连接数
	private int minCon ;
	// 数据库类型
	private String dbType;
	//
	private String filters="mergeStat";
	//
	private long logTime = 300000;
	// 权重
	private int weight;				

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public DBHostConfig(String hostName, String ip, int port, String url,
			String user, String password,String encryptPassword, boolean useSSL) {
		super();
		this.hostName = hostName;
		this.ip = ip;
		this.port = port;
		this.url = url;
		this.user = user;
		this.password = password;
		this.encryptPassword = encryptPassword;
		this.useSSL = useSSL;
	}

	public boolean isUseSSL() {
		return useSSL;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
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

	public String getHostName() {
		return hostName;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public String getUrl() {
		return url;
	}

	public String getUser() {
		return user;
	}
	public String getFilters() {
		return filters;
	}

	public void setFilters(String filters) {
		this.filters = filters;
	}
	public String getPassword() {
		return password;
	}

	public long getLogTime() {
		return logTime;
	}

	public void setLogTime(long logTime) {
		this.logTime = logTime;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getEncryptPassword() {
		return this.encryptPassword;
	}

	@Override
	public String toString() {
		return "DBHostConfig [hostName=" + hostName + ", url=" + url + "]";
	}

}