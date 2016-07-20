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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.opencloudb.config.model.rule.RuleAlgorithm;

/**
 * number column partion by Mod operator
 * if count is 10 then 0 to 0,21 to 1 (21 % 10 =1)
 * @author wuzhih
 *
 */
public class PartitionByMod extends AbstractPartitionAlgorithm implements RuleAlgorithm  {

	private int count;
	@Override
	public void init() {
	
		
	}



	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public Integer calculate(String columnValue) {
		columnValue = NumberParseUtil.eliminateQoute(columnValue);
	BigInteger bigNum = new BigInteger(columnValue).abs();
	 return (bigNum.mod(BigInteger.valueOf(count))).intValue();
	}

	private static void hashTest(){
		PartitionByMod hash=new PartitionByMod();
		hash.setCount(11);
		hash.init();
		
		int[] bucket=new int[hash.count];
		
		Map<Integer,List<Integer>> hashed=new HashMap<>();
		
		int total=1000_0000;//数据量
		int c=0;
		for(int i=100_0000;i<total+100_0000;i++){//假设分片键从100万开始
			c++;
			int h=hash.calculate(Integer.toString(i));
			bucket[h]++;
			List<Integer> list=hashed.get(h);
			if(list==null){
				list=new ArrayList<>();
				hashed.put(h, list);
			}
			list.add(i);
		}
		System.out.println(c+"   "+total);
		double d=0;
		c=0;
		int idx=0;
		System.out.println("index    bucket   ratio");
		for(int i:bucket){
			d+=i/(double)total;
			c+=i;
			System.out.println(idx+++"  "+i+"   "+(i/(double)total));
		}
		System.out.println(d+"  "+c);
		
		System.out.println("****************************************************");
		rehashTest(hashed.get(0));
	}
	private static void rehashTest(List<Integer> partition){
		PartitionByMod hash=new PartitionByMod();
		hash.count=110;//分片数
		hash.init();
		
		int[] bucket=new int[hash.count];
		
		int total=partition.size();//数据量
		int c=0;
		for(int i:partition){//假设分片键从100万开始
			c++;
			int h=hash.calculate(Integer.toString(i));
			bucket[h]++;
		}
		System.out.println(c+"   "+total);
		c=0;
		int idx=0;
		System.out.println("index    bucket   ratio");
		for(int i:bucket){
			c+=i;
			System.out.println(idx+++"  "+i+"   "+(i/(double)total));
		}
	}
	public static void main(String[] args) {
		hashTest();
	}
}