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
package org.opencloudb.route.function;

import org.opencloudb.config.model.rule.RuleAlgorithm;
import org.opencloudb.route.util.PartitionUtil;

public final class PartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {
	protected int[] count;
	protected int[] length;
	protected PartitionUtil partitionUtil;

	private static int[] toIntArray(String string) {
		String[] strs = org.opencloudb.util.SplitUtil.split(string, ',', true);
		int[] ints = new int[strs.length];
		for (int i = 0; i < strs.length; ++i) {
			ints[i] = Integer.parseInt(strs[i]);
		}
		return ints;
	}

	public void setPartitionCount(String partitionCount) {
		this.count = toIntArray(partitionCount);
	}

	public void setPartitionLength(String partitionLength) {
		this.length = toIntArray(partitionLength);
	}

	@Override
	public void init() {
		partitionUtil = new PartitionUtil(count, length);

	}

	@Override
	public Integer calculate(String columnValue) {
		columnValue = NumberParseUtil.eliminateQoute(columnValue);
		long key = Long.parseLong(columnValue);
		return partitionUtil.partition(key);
	}
	
	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return AbstractPartitionAlgorithm.calculateSequenceRange(this, beginValue, endValue);
	}

}