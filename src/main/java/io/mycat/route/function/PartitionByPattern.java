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
package io.mycat.route.function;


import java.util.Set;
import java.util.regex.Pattern;

/**
 * auto partition by Long
 *
 * @author hexiaobin
 */
public class PartitionByPattern extends AbstractPartitionAlgorithm implements RuleAlgorithm {
	private static final int PARTITION_LENGTH = 1024;
	private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值
	private LongRange[] longRongs;
	private int defaultNode = 0;// 包含非数值字符，默认存储节点
    private static final  Pattern pattern = Pattern.compile("[0-9]*");

	@Override
	public void init() {
		initialize();
	}

	public void setPatternValue(int patternValue) {
		this.patternValue = patternValue;
	}
	public void setDefaultNode(int defaultNode) {
		this.defaultNode = defaultNode;
	}

	@Override
	public Integer calculate(String columnValue) {
		if (!isNumeric(columnValue)) {
			return defaultNode;
		}
		long value = Long.valueOf(columnValue);
		Integer rst = null;
		for (LongRange longRang : this.longRongs) {
			long hash = value % patternValue;
			if (hash <= longRang.valueEnd && hash >= longRang.valueStart) {
				return longRang.nodeIndx;
			}
		}
		return rst;
	}

	public static boolean isNumeric(String str) {
		return pattern.matcher(str).matches();
	}

	private void initialize() {
		if (this.getConfig().isEmpty()) {
			throw new RuntimeException("can't find ByPattern config, like <config> <property name=\"1-4\">0</property> </config>");
		}
		longRongs = new LongRange[this.getConfig().size()];
		Set<String> keys = this.getConfig().keySet();
		int i=0;
		for(String key : keys){
			String pairs[] = key.trim().split("-");
			long longStart = NumberParseUtil.parseLong(pairs[0].trim());
			long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
			int nodeId = Integer.parseInt((String)this.getConfig().get(key));
			longRongs[i] = new LongRange(nodeId, longStart, longEnd);
			i++;
		}
	}

	static class LongRange {
		public final int nodeIndx;
		public final long valueStart;
		public final long valueEnd;

		public LongRange(int nodeIndx, long valueStart, long valueEnd) {
			super();
			this.nodeIndx = nodeIndx;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}

	}
}