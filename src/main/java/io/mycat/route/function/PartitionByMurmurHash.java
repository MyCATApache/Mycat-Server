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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import io.mycat.server.exception.MurmurHashException;

/**
 * consistancy hash, murmur hash
 * implemented by Guava
 * @author wuzhih
 *
 */
public class PartitionByMurmurHash extends AbstractPartitionAlgorithm implements RuleAlgorithm  {
	private static final int DEFAULT_VIRTUAL_BUCKET_TIMES=160;

	private int seed;
	private int count;
	private int virtualBucketTimes=DEFAULT_VIRTUAL_BUCKET_TIMES;

	private HashFunction hash;

	private SortedMap<Integer,Integer> bucketMap;
	@Override
	public void init()  {
		try{
			bucketMap=new TreeMap<>();
			generateBucketMap();
		}catch(Exception e){
			throw new MurmurHashException(e);
		}
	}

	private void generateBucketMap(){
		hash=Hashing.murmur3_32(seed);//计算一致性哈希的对象
		for(int i=0;i<count;i++){//构造一致性哈希环，用TreeMap表示
			StringBuilder hashName=new StringBuilder("SHARD-").append(i);
			for(int n=0,shard=virtualBucketTimes;n<shard;n++){
				bucketMap.put(hash.hashUnencodedChars(hashName.append("-NODE-").append(n)).asInt(),i);
			}
		}
	}
	/**
	 * 创建murmur_hash对象的种子，默认0
	 * @param seed
	 */
	public void setSeed(int seed){
		this.seed=seed;
	}
	/**
	 * 节点的数量
	 * @param count
	 */
	public void setCount(int count) {
		this.count = count;
	}
	/**
	 * 虚拟节点倍数，virtualBucketTimes*count就是虚拟结点数量
	 * @param virtualBucketTimes
	 */
	public void setVirtualBucketTimes(int virtualBucketTimes){
		this.virtualBucketTimes=virtualBucketTimes;
	}
	@Override
	public Integer calculate(String columnValue) {
		SortedMap<Integer, Integer> tail = bucketMap.tailMap(hash.hashUnencodedChars(columnValue).asInt());
		if (tail.isEmpty()) {
		    return bucketMap.get(bucketMap.firstKey());
		}
		return tail.get(tail.firstKey());
	}

	private static void hashTest() throws IOException{
		PartitionByMurmurHash hash=new PartitionByMurmurHash();
		hash.count=1000;//分片数
		hash.init();

		int[] bucket=new int[hash.count];

		Map<Integer,List<Integer>> hashed=new HashMap<>();

		int total=1000_0000;//数据量
		int c=0;
		for(int i=100_0000;i<total+100_000;i++){//假设分片键从100万开始
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

		Properties props=new Properties();
		for(Map.Entry entry:hash.bucketMap.entrySet()){
			props.setProperty(entry.getKey().toString(), entry.getValue().toString());
		}
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		props.store(out, null);

		props.clear();
		props.load(new ByteArrayInputStream(out.toByteArray()));
		System.out.println(props);
		System.out.println("****************************************************");
//		rehashTest(hashed.get(0));
	}
	private static void rehashTest(List<Integer> partition){
		PartitionByMurmurHash hash=new PartitionByMurmurHash();
		hash.count=12;//分片数
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
	public static void main(String[] args) throws IOException {
		hashTest();
	}
}
