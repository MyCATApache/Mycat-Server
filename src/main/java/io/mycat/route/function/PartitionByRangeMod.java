/*
 * Copyright (c) 2015, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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
 * https://github.com/MyCATApache/Mycat-Server.
 *
 */
package io.mycat.route.function;


import java.math.BigInteger;
import java.util.Set;

/**
 * 先进行范围分片计算出分片组，组内再取模
 * 优点可以避免扩容时的数据迁移，又可以一定程度上避免范围分片的热点问题
 *
 * @author wuzhi
 */
public class PartitionByRangeMod extends AbstractPartitionAlgorithm implements RuleAlgorithm{

	private LongRange[] longRanges;
	private int defaultNode = -1;

	@Override
	public void init() {
		initialize();
	}

	@Override
	public Integer calculate(String columnValue) {
		long value = Long.valueOf(columnValue);
		Integer rst = null;
        int nodeIndex=0;
		for (LongRange longRang : this.longRanges) {
			if (value <= longRang.valueEnd && value >= longRang.valueStart) {
                BigInteger bigNum = new BigInteger(columnValue).abs();
                int innerIndex= (bigNum.mod(BigInteger.valueOf(longRang.groupSize))).intValue();
				return nodeIndex+innerIndex;
			}    else
            {
                nodeIndex+= longRang.groupSize;
            }
		}
		//数据超过范围，暂时使用配置的默认节点
		if(rst ==null && defaultNode>=0){
			return defaultNode ;
		}
		return rst;
	}

    public Integer calculateStart(String columnValue) {
        long value = Long.valueOf(columnValue);
        Integer rst = null;
        int nodeIndex=0;
        for (LongRange longRang : this.longRanges) {
            if (value <= longRang.valueEnd && value >= longRang.valueStart) {

                return nodeIndex;
            }    else
            {
                nodeIndex+= longRang.groupSize;
            }
        }
        // 数据超过范围，暂时使用配置的默认节点
        if(rst ==null && defaultNode>=0){
            return defaultNode ;
        }
        return rst;
    }
    public Integer calculateEnd(String columnValue) {
        long value = Long.valueOf(columnValue);
        Integer rst = null;
        int nodeIndex=0;
        for (LongRange longRang : this.longRanges) {
            if (value <= longRang.valueEnd && value >= longRang.valueStart) {

                return nodeIndex+longRang.groupSize -1;
            }    else
            {
                nodeIndex+= longRang.groupSize;
            }
        }
        // 数据超过范围，暂时使用配置的默认节点
        if(rst ==null && defaultNode>=0){
            return defaultNode ;
        }
        return rst;
    }

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
        Integer begin = 0, end = 0;
        begin = calculateStart(beginValue);
        end = calculateEnd(endValue);

        if(begin == null || end == null){
            return new Integer[0];
        }

        if (end >= begin) {
            int len = end-begin+1;
            Integer [] re = new Integer[len];

            for(int i =0;i<len;i++){
                re[i]=begin+i;
            }

            return re;
        }else{
            return null;
        }
	}



	private void initialize() {
		if (this.getConfig().isEmpty()) {
			throw new RuntimeException("can't find PartitionByRangeMod config, like <config> <property name=\"1-4\">0</property> </config>");
		}
		longRanges = new LongRange[this.getConfig().size()];
		Set<String> keys = this.getConfig().keySet();
		int i=0;
		for(String key : keys){
			String pairs[] = key.trim().split("-");
			long longStart = NumberParseUtil.parseLong(pairs[0].trim());
			long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
			int nodeId = Integer.parseInt((String)this.getConfig().get(key));
			longRanges[i] = new LongRange(nodeId, longStart, longEnd);
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
		public final int groupSize;
		public final long valueStart;
		public final long valueEnd;

		public LongRange(int groupSize, long valueStart, long valueEnd) {
			super();
			this.groupSize = groupSize;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}

	}
}
