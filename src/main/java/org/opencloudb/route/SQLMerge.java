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

import org.opencloudb.mpp.HavingCols;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class SQLMerge implements Serializable {
	private LinkedHashMap<String, Integer> orderByCols;
	private HavingCols havingCols;
	private Map<String, Integer> mergeCols;
	private String[] groupByCols;
	private boolean hasAggrColumn;

	public LinkedHashMap<String, Integer> getOrderByCols() {
		return orderByCols;
	}

	public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) {
		this.orderByCols = orderByCols;
	}

	public Map<String, Integer> getMergeCols() {
		return mergeCols;
	}

	public void setMergeCols(Map<String, Integer> mergeCols) {
		this.mergeCols = mergeCols;
	}

	public String[] getGroupByCols() {
		return groupByCols;
	}

	public void setGroupByCols(String[] groupByCols) {
		this.groupByCols = groupByCols;
	}

	public boolean isHasAggrColumn() {
		return hasAggrColumn;
	}

	public void setHasAggrColumn(boolean hasAggrColumn) {
		this.hasAggrColumn = hasAggrColumn;
	}

	public HavingCols getHavingCols() {
		return havingCols;
	}

	public void setHavingCols(HavingCols havingCols) {
		this.havingCols = havingCols;
	}
}