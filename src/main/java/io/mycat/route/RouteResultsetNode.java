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
package io.mycat.route;

import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.LoadData;

import java.io.Serializable;

/**
 * @author mycat
 */
public final class RouteResultsetNode implements Serializable , Comparable<RouteResultsetNode> {
	private static final long serialVersionUID = 1L;
	private final String name; // 数据节点名称
	private String statement; // 执行的语句
	private final String srcStatement;
	private final int sqlType;
	private volatile boolean canRunInReadDB;
	private final boolean hasBlanceFlag;
	
	// 强制走 master，强制走 slave统一使用该属性来标志，true走slave，false走master
	private Boolean runOnSlave = null;	// 默认null表示不施加影响

	private int limitStart;
	private int limitSize;
	private int totalNodeSize =0; //方便后续jdbc批量获取扩展

	private LoadData loadData;

	public RouteResultsetNode(String name, int sqlType, String srcStatement) {
		this.name = name;
		limitStart=0;
		this.limitSize = -1;
		this.sqlType = sqlType;
		this.srcStatement = srcStatement;
		this.statement = srcStatement;
		canRunInReadDB = (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW);
		hasBlanceFlag = (statement != null)
				&& statement.startsWith("/*balance*/");
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}

	public void setCanRunInReadDB(boolean canRunInReadDB) {
		this.canRunInReadDB = canRunInReadDB;
	}

	public boolean getCanRunInReadDB() {
		return this.canRunInReadDB;
	}

	public void resetStatement() {
		this.statement = srcStatement;
	}

	/**
	 * 在没有使用 db_type=master/slave 注解时，该函数用来判断是否可以进行负载均衡，其逻辑为：
	 * 在mysql client, heartbeat, SQLJob等执行一些非业务SQL时，使用的是被Leader-us
	 * 优化过的query函数，其默认是 autocommit=true; 
	 * 
	 * 所以如果是 select或者show语句(canRunInReadDB=true)，并且 autocommit=true(非业务sql)，
	 * 那么就可以进行负载均衡执行sql，可以在slave上执行
	 * 如果是select或者show语句，但是是业务sql(aotucommit=false)，但是该sql
	 * 被 balance 注解了，那么也可以进行负载均衡，可以在slave上执行
	 * 
	 * @param autocommit
	 * @return
	 */
	public boolean canRunnINReadDB(boolean autocommit) {
		return canRunInReadDB && ( autocommit || (!autocommit && hasBlanceFlag) );
	}
	
//	public boolean canRunnINReadDB(boolean autocommit) {
//		return canRunInReadDB && autocommit && !hasBlanceFlag
//			|| canRunInReadDB && !autocommit && hasBlanceFlag;
//	}

	public Boolean getRunOnSlave() {
		return runOnSlave;
	}

	public void setRunOnSlave(Boolean runOnSlave) {
		this.runOnSlave = runOnSlave;
	}

	public String getName() {
		return name;
	}

	public int getSqlType() {
		return sqlType;
	}

	public String getStatement() {
		return statement;
	}

	public int getLimitStart()
	{
		return limitStart;
	}

	public void setLimitStart(int limitStart)
	{
		this.limitStart = limitStart;
	}

	public int getLimitSize()
	{
		return limitSize;
	}

	public void setLimitSize(int limitSize)
	{
		this.limitSize = limitSize;
	}

	public int getTotalNodeSize()
	{
		return totalNodeSize;
	}

	public void setTotalNodeSize(int totalNodeSize)
	{
		this.totalNodeSize = totalNodeSize;
	}

	public LoadData getLoadData()
	{
		return loadData;
	}

	public void setLoadData(LoadData loadData)
	{
		this.loadData = loadData;
	}

	public boolean isHasBlanceFlag() {
		return hasBlanceFlag;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof RouteResultsetNode) {
			RouteResultsetNode rrn = (RouteResultsetNode) obj;
			if (equals(name, rrn.getName())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(name);
		s.append('{').append(statement).append('}');
		return s.toString();
	}

	private static boolean equals(String str1, String str2) {
		if (str1 == null) {
			return str2 == null;
		}
		return str1.equals(str2);
	}

	public boolean isModifySQL() {
		return !canRunInReadDB;
	}

	@Override
	public int compareTo(RouteResultsetNode obj) {
		if(obj == null) {
			return 1;
		}
		if(this.name == null) {
			return -1;
		}
		if(obj.name == null) {
			return 1;
		}
		return this.name.compareTo(obj.name);
	}
}
