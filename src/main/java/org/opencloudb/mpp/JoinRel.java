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
package org.opencloudb.mpp;

/**
 * join relation
 * 
 * @author wuzhih
 * 
 */
public class JoinRel {
	public JoinRel(String leftTable, String columnNameA, String rightTable,
			String columnNameB) {
		tableA=leftTable;
		columnA=columnNameA.toUpperCase();
		tableB=rightTable;
		columnB=columnNameB.toUpperCase();
		joinSQLExp=tableA+'.'+columnA+'='+tableB+'.'+columnB;
	}
	
	public final String joinSQLExp;
	public final String tableA;
	public final String columnA;
	public final String tableB;
	public final String columnB;
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnA == null) ? 0 : columnA.hashCode());
		result = prime * result + ((columnB == null) ? 0 : columnB.hashCode());
		result = prime * result + ((tableA == null) ? 0 : tableA.hashCode());
		result = prime * result + ((tableB == null) ? 0 : tableB.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JoinRel other = (JoinRel) obj;
		if (columnA == null) {
			if (other.columnA != null)
				return false;
		} else if (!columnA.equals(other.columnA))
			return false;
		if (columnB == null) {
			if (other.columnB != null)
				return false;
		} else if (!columnB.equals(other.columnB))
			return false;
		if (tableA == null) {
			if (other.tableA != null)
				return false;
		} else if (!tableA.equals(other.tableA))
			return false;
		if (tableB == null) {
			if (other.tableB != null)
				return false;
		} else if (!tableB.equals(other.tableB))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "JoinRel [tableA=" + tableA + ", columnA=" + columnA
				+ ", tableB=" + tableB + ", columnB=" + columnB + "]";
	}
	
	
}