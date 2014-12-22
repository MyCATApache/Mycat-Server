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
package org.opencloudb.route;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.opencloudb.util.FormatUtil;

/**
 * @author mycat
 */
public final class RouteResultset implements Serializable {
	private String statement; // 原始语句
	private final int sqlType;
	private RouteResultsetNode[] nodes; // 路由结果节点

	private int limitStart;
	private boolean cacheAble;
	// used to store table's ID->datanodes cache
	// format is table.primaryKey
	private String primaryKey;
	// limit output total
	private int limitSize;
	private SQLMerge sqlMerge;

	private boolean callStatement = false; // 处理call关键字

	// 是否为全局表，只有在insert、update、delete、ddl里会判断并修改。默认不是全局表，用于修正全局表修改数据的反馈。
	private boolean globalTableFlag = false;
	
	//是否完成了路由
	private boolean isFinishedRoute = false;
	
	public boolean isFinishedRoute() {
		return isFinishedRoute;
	}

	public void setFinishedRoute(boolean isFinishedRoute) {
		this.isFinishedRoute = isFinishedRoute;
	}

	public boolean isGlobalTable() {
		return globalTableFlag;
	}

	public void setGlobalTable(boolean globalTableFlag) {
		this.globalTableFlag = globalTableFlag;
	}

	public RouteResultset(String stmt, int sqlType) {
		this.statement = stmt;
		this.limitSize = -1;
		this.sqlType = sqlType;
	}

	public void resetNodes() {
		if (nodes != null) {
			for (RouteResultsetNode node : nodes) {
				node.resetStatement();
			}
		}
	}

	public SQLMerge getSqlMerge() {
		return sqlMerge;
	}

	public boolean isCacheAble() {
		return cacheAble;
	}

	public void setCacheAble(boolean cacheAble) {
		this.cacheAble = cacheAble;
	}

	public boolean needMerge() {
		return limitSize > 0 || sqlMerge != null;
	}

	public int getSqlType() {
		return sqlType;
	}

	public boolean isHasAggrColumn() {
		return (sqlMerge != null) && sqlMerge.isHasAggrColumn();
	}

	public int getLimitStart() {
		return limitStart;
	}

	public String[] getGroupByCols() {
		return (sqlMerge != null) ? sqlMerge.getGroupByCols() : null;
	}

	private SQLMerge createSQLMergeIfNull() {
		if (sqlMerge == null) {
			sqlMerge = new SQLMerge();
		}
		return sqlMerge;
	}

	public Map<String, Integer> getMergeCols() {
		return (sqlMerge != null) ? sqlMerge.getMergeCols() : null;
	}

	public void setLimitStart(int limitStart) {
		this.limitStart = limitStart;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public boolean hasPrimaryKeyToCache() {
		return primaryKey != null;
	}

	public void setPrimaryKey(String primaryKey) {
		if (!primaryKey.contains(".")) {
			throw new java.lang.IllegalArgumentException(
					"must be table.primarykey fomat :" + primaryKey);
		}
		this.primaryKey = primaryKey;
	}

	/**
	 * return primary key items ,first is table name ,seconds is primary key
	 * 
	 * @return
	 */
	public String[] getPrimaryKeyItems() {
		return primaryKey.split("\\.");
	}

	public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) {
		if (orderByCols != null && !orderByCols.isEmpty()) {
			createSQLMergeIfNull().setOrderByCols(orderByCols);
		}
	}

	public void setHasAggrColumn(boolean hasAggrColumn) {
		if (hasAggrColumn) {
			createSQLMergeIfNull().setHasAggrColumn(true);
		}
	}

	public void setGroupByCols(String[] groupByCols) {
		if (groupByCols != null && groupByCols.length > 0) {
			createSQLMergeIfNull().setGroupByCols(groupByCols);
		}
	}

	public void setMergeCols(Map<String, Integer> mergeCols) {
		if (mergeCols != null && !mergeCols.isEmpty()) {
			createSQLMergeIfNull().setMergeCols(mergeCols);
		}

	}

	public LinkedHashMap<String, Integer> getOrderByCols() {
		return (sqlMerge != null) ? sqlMerge.getOrderByCols() : null;

	}

	public String getStatement() {
		return statement;
	}

	public RouteResultsetNode[] getNodes() {
		return nodes;
	}

	public void setNodes(RouteResultsetNode[] nodes) {
		this.nodes = nodes;
	}

	/**
	 * @return -1 if no limit
	 */
	public int getLimitSize() {
		return limitSize;
	}

	public void setLimitSize(int limitSize) {
		this.limitSize = limitSize;
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}

	public boolean isCallStatement() {
		return callStatement;
	}

	public void setCallStatement(boolean callStatement) {
		this.callStatement = callStatement;
	}
	
	public void changeNodeSqlAfterAddLimit(String sql) {
		if (nodes != null) {
			for (RouteResultsetNode node : nodes) {
				node.setStatement(sql);
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(statement).append(", route={");
		if (nodes != null) {
			for (int i = 0; i < nodes.length; ++i) {
				s.append("\n ").append(FormatUtil.format(i + 1, 3));
				s.append(" -> ").append(nodes[i]);
			}
		}
		s.append("\n}");
		return s.toString();
	}

}