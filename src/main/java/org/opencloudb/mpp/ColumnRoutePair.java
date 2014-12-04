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
 * column ->node index
 * 
 * @author wuzhih
 * 
 */
public class ColumnRoutePair {
	public final String colValue;
	public final RangeValue rangeValue;
	public Integer nodeId;

	public ColumnRoutePair(String colValue) {
		super();
		this.colValue = colValue;
		this.rangeValue = null;
	}

	public ColumnRoutePair(RangeValue rangeValue) {
		super();
		this.rangeValue = rangeValue;
		this.colValue = null;
	}

	public Integer getNodeId() {
		return nodeId;
	}

	public void setNodeId(Integer nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((colValue == null) ? 0 : colValue.hashCode());
		result = prime * result
				+ ((rangeValue == null) ? 0 : rangeValue.hashCode());
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
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
		ColumnRoutePair other = (ColumnRoutePair) obj;
		if (colValue == null) {
			if (other.colValue != null)
				return false;
		} else if (!colValue.equals(other.colValue))
			return false;

		if (rangeValue == null) {
			if (other.rangeValue != null) {
				return false;
			}
		} else if (!rangeValue.equals(other.rangeValue)) {
			return false;
		}

		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ColumnRoutePair [colValue=" + colValue + ", nodeId=" + nodeId
				+ "]";
	}
}