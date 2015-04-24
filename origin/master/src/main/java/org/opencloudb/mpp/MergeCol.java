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

public class MergeCol {
	public static final int MERGE_COUNT = 1;
	public static final int MERGE_SUM = 2;
	public static final int MERGE_MIN = 3;
	public static final int MERGE_MAX = 4;
    public static final int MERGE_AVG= 5;
	public static final int MERGE_UNSUPPORT = -1;
	public static final int MERGE_NOMERGE = -2;
	public final int mergeType;
	public final ColMeta colMeta;

	public MergeCol(ColMeta colMeta, int mergeType) {
		super();
		this.colMeta = colMeta;
		this.mergeType = mergeType;
	}

	public static int getMergeType(String mergeType) {
		String upper=mergeType.toUpperCase();
		if (upper.startsWith("COUNT")) {
			return MERGE_COUNT;
		} else if (upper.startsWith("SUM")) {
			return MERGE_SUM;
		} else if (upper.startsWith("MIN")) {
			return MERGE_MIN;
		} else if (upper.startsWith("MAX")) {
			return MERGE_MAX;
		}
        else if (upper.startsWith("AVG")) {
            return MERGE_AVG;
        }
        else {
			return MERGE_UNSUPPORT;
		}
	}

	public static int tryParseAggCol(String column) {
		// MIN(*),MAX(*),COUNT(*),SUM
		if (column.length() < 6) {
			return -1;
		}
		column = column.toUpperCase();

		if (column.startsWith("COUNT(")) {
			return MERGE_COUNT;
		} else if (column.startsWith("SUM(")) {
			return MERGE_SUM;
		} else if (column.startsWith("MIN(")) {
			return MERGE_MIN;
		} else if (column.startsWith("MAX(")) {
			return MERGE_MAX;
		} else if (column.startsWith("AVG(")) {
			return MERGE_AVG;
		} else {
			return MERGE_NOMERGE;
		}
	}
}