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

import io.mycat.config.model.rule.RuleAlgorithm;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.LinkedList;

/**
 * 先进行范围分片计算出分片组，组内再取模
 * 优点可以避免扩容时的数据迁移，又可以一定程度上避免范围分片的热点问题
 * 
 * @author wuzhi
 */
public class PartitionByRangeMod extends AbstractPartitionAlgorithm implements RuleAlgorithm{

	private String mapFile;
	private LongRange[] longRanges;
	
	private int defaultNode = -1;
	@Override
	public void init() {

		initialize();
	}

	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}

	@Override
	public Integer calculate(String columnValue)  {
//		columnValue = NumberParseUtil.eliminateQoute(columnValue);
		try {
			long value = Long.parseLong(columnValue);
			Integer rst = null;
			int nodeIndex = 0;
			for (LongRange longRang : this.longRanges) {
				if (value <= longRang.valueEnd && value >= longRang.valueStart) {
					BigInteger bigNum = new BigInteger(columnValue).abs();
					int innerIndex = (bigNum.mod(BigInteger.valueOf(longRang.groupSize))).intValue();
					return nodeIndex + innerIndex;
				} else {
					nodeIndex += longRang.groupSize;
				}
			}
			//数据超过范围，暂时使用配置的默认节点
			if (rst == null && defaultNode >= 0) {
				return defaultNode;
			}
			return rst;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please eliminate any quote and non number within it.").toString(), e);
		}
	}
    
	@Override
	public int getPartitionNum() {
		int nPartition = 0;
		for(LongRange longRange : this.longRanges) {
			nPartition += longRange.groupSize;
		}
		return nPartition;
	}

	public Integer calculateStart(String columnValue) {
        long value = Long.parseLong(columnValue);
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
        long value = Long.parseLong(columnValue);
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
		BufferedReader in = null;
		try {
			InputStream fin = this.getClass().getClassLoader()
					.getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file "
						+ mapFile);
			}
			in = new BufferedReader(new InputStreamReader(fin));
			LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();

			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				int ind = line.indexOf('=');
				if (ind < 0) {
					System.out.println(" warn: bad line int " + mapFile + " :"
							+ line);
					continue;
				}
					String pairs[] = line.substring(0, ind).trim().split("-");
					long longStart = NumberParseUtil.parseLong(pairs[0].trim());
					long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
					int nodeId = Integer.parseInt(line.substring(ind + 1)
							.trim());
					longRangeList
							.add(new LongRange(nodeId, longStart, longEnd));

			}
			longRanges = longRangeList.toArray(new LongRange[longRangeList.size()]);
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}

		} finally {
			try {
				in.close();
			} catch (Exception e2) {
			}
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
