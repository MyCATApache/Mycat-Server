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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.opencloudb.config.model.rule.RuleAlgorithm;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 * 
 * @author wuzhi
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm{
	
	private static final String NODES_SPLITOR=",";
	
	private String mapFile;
	private LongRange[] longRongs;
	
	private int defaultNode = -1;
	@Override
	public void init() {

		initialize();
	}
	/**
	 * partition field map file 
	 * the format is like this:
	 * 1-100M=0,1,2,3
	 * 100M1-200M=4
	 * 200M1-300M=5,6
	 * M means 10000
	 * @param mapFile
	 */
	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}

	@Override
	public Integer calculate(String columnValue) {
		long value = Long.valueOf(columnValue);
		Integer rst = null;
		for (LongRange longRang : this.longRongs) {
			if (value <= longRang.valueEnd && value >= longRang.valueStart) {
				int[] nodes=longRang.nodes;
				return nodes[(int)(value%nodes.length)];
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
		BufferedReader in = null;
		try {
			// FileInputStream fin = new FileInputStream(new File(fileMapPath));
			InputStream fin = this.getClass().getClassLoader().getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file "
						+ mapFile);
			}
			in = new BufferedReader(new InputStreamReader(fin));
			LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();

			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//"))
					continue;
				int ind = line.indexOf('=');
				if (ind < 0) {
					System.out.println(" warn: bad line int " + mapFile + " :"
							+ line);
					continue;
				}
				try {
					String pairs[] = line.substring(0, ind).trim().split("-");
					long longStart = NumberParseUtil.parseLong(pairs[0].trim());
					long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
					longRangeList.add(new LongRange(parseNodes(line.substring(ind + 1).trim()),longStart, longEnd));

				} catch (Exception e) {
				}
			}
			longRongs = longRangeList.toArray(new LongRange[longRangeList
					.size()]);
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
	private int[] parseNodes(String nodesTxt){
		StringTokenizer st=new StringTokenizer(nodesTxt,NODES_SPLITOR);
		int[] nodes=new int[st.countTokens()];
		for(int i=0,l=nodes.length;i<l;i++){
			nodes[i]=Integer.parseInt(st.nextToken().trim());
		}
		return nodes;
	}
	
	public int getDefaultNode() {
		return defaultNode;
	}

	public void setDefaultNode(int defaultNode) {
		this.defaultNode = defaultNode;
	}

	static class LongRange {
		public final long valueStart;
		public final long valueEnd;
		public final int[] nodes;

		public LongRange(int[] nodes, long valueStart, long valueEnd) {
			this.nodes=nodes;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}

	}
}