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
package io.mycat.cache;

import io.mycat.cache.CachePool;
import io.mycat.cache.CacheStatic;
import io.mycat.cache.impl.EnchachePool;
import io.mycat.cache.impl.MapDBCachePooFactory;
/**
 * test cache performance ,for encache test set  VM param  -server -Xms1100M -Xmx1100M
 * for mapdb set vm param -server -Xms100M -Xmx100M -XX:MaxPermSize=1G
 */
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;

public class TestCachePoolPerformance {
	private CachePool pool;
	private int maxCacheCount = 100 * 10000;

	public static CachePool createEnCachePool() {
		CacheConfiguration cacheConf = new CacheConfiguration();
		cacheConf.setName("testcache");
		cacheConf.maxBytesLocalHeap(400, MemoryUnit.MEGABYTES)
				.timeToIdleSeconds(3600);
		Cache cache = new Cache(cacheConf);
		CacheManager.create().addCache(cache);
		EnchachePool enCachePool = new EnchachePool(cacheConf.getName(),cache,400*10000);
		return enCachePool;
	}

	public static CachePool createMapDBCachePool() {
		MapDBCachePooFactory fact = new MapDBCachePooFactory();
		return fact.createCachePool("mapdbcache", 100 * 10000, 3600);

	}

	public void test() {
		testSwarm();
		testInsertSpeed();
		testSelectSpeed();
	}

	private void testSwarm() {
		System.out.println("prepare ........");
		for (int i = 0; i < 100000; i++) {
			pool.putIfAbsent(i % 100, "dn1");
		}
		for (int i = 0; i < 100000; i++) {
			pool.get(i % 100);
		}
		pool.clearCache();
	}

	private void testSelectSpeed() {
		System.out.println("test select speed for " + this.pool + " count:"
				+ this.maxCacheCount);
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < maxCacheCount; i++) {
			pool.get(i + "");
		}
		double used = (System.currentTimeMillis() - startTime) / 1000.0;
		CacheStatic statics = pool.getCacheStatic();
		System.out.println("used time:" + used + " tps:" + maxCacheCount / used
				+ " cache hit:" + 100 * statics.getHitTimes()
				/ statics.getAccessTimes());
	}

	private void GC() {
		for (int i = 0; i < 5; i++) {
			System.gc();
		}
	}

	private void testInsertSpeed() {
		this.GC();
		long freeMem = Runtime.getRuntime().freeMemory();
		System.out.println("test insert speed for " + this.pool
				+ " with insert count:" + this.maxCacheCount);
		long start = System.currentTimeMillis();
		for (int i = 0; i < maxCacheCount; i++) {
			try {
				pool.putIfAbsent(i + "", "dn" + i % 100);
			} catch (Error e) {
				System.out.println("insert " + i + " error");
				e.printStackTrace();
				break;
			}
		}
		long used = (System.currentTimeMillis() - start) / 1000;
		long count = pool.getCacheStatic().getItemSize();
		this.GC();
		long usedMem = freeMem - Runtime.getRuntime().freeMemory();
		System.out.println(" cache size is " + count + " ,all in cache :"
				+ (count == maxCacheCount) + " ,used time:" + used + " ,tps:"
				+ count / used + " used memory:" + usedMem / 1024 / 1024 + "M");
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out
					.println("usage : \r\n cache: 1 for encache 2 for mapdb\r\n");
			return;
		}
		TestCachePoolPerformance tester = new TestCachePoolPerformance();
		int cacheType = Integer.parseInt(args[0]);
		if (cacheType == 1) {
			tester.pool = createEnCachePool();
			tester.test();
		} else if (cacheType == 2) {
			tester.pool = createMapDBCachePool();
			tester.test();
		} else {
			System.out.println("not valid input ");
		}

	}
}