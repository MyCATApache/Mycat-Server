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

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 *
 * @author wuzhi
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm{

	private LongRange[] longRongs;
	private int defaultNode = -1;


	@Override
	public void init() {
		initialize();
	}

	@Override
	public Integer calculate(String columnValue) {
		long value = Long.valueOf(columnValue);
		Integer rst = null;
		for (LongRange longRang : this.longRongs) {
			if (value <= longRang.valueEnd && value >= longRang.valueStart) {
				return longRang.nodeIndx;
			}
		}
		//数据超过范围，暂时使用配置的默认节点
		if(rst ==null && defaultNode>=0){
			return defaultNode ;
		}
		return rst;
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return AbstractPartitionAlgorithm.calculateSequenceRange(this, beginValue, endValue);
	}

	private void initialize() {
		if (this.getConfig().isEmpty()) {
			throw new RuntimeException("can't find range config, like <config> <property name=\"1-4\">0</property> </config>");
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

	public int getDefaultNode() {
		return defaultNode;
	}

	public void setDefaultNode(int defaultNode) {
		this.defaultNode = defaultNode;
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