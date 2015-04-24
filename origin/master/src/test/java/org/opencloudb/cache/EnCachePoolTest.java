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
package org.opencloudb.cache;

import junit.framework.Assert;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;

import org.junit.Test;
import org.opencloudb.cache.impl.EnchachePool;

public class EnCachePoolTest {

	private static EnchachePool enCachePool;
	static {
		CacheConfiguration cacheConf = new CacheConfiguration();
		cacheConf.setName("testcache");
		cacheConf.maxBytesLocalHeap(50,MemoryUnit.MEGABYTES).timeToIdleSeconds(2);
		Cache cache=new Cache(cacheConf);
		CacheManager.create().addCache(cache);
		enCachePool = new EnchachePool(cacheConf.getName(),cache,50*10000);
	}

	@Test
	public void testBasic() {
		enCachePool.putIfAbsent("2", "dn2");
		enCachePool.putIfAbsent("1", "dn1");

		Assert.assertEquals("dn2", enCachePool.get("2"));
		Assert.assertEquals("dn1", enCachePool.get("1"));
		Assert.assertEquals(null, enCachePool.get("3"));

		CacheStatic statics = enCachePool.getCacheStatic();
		Assert.assertEquals(statics.getItemSize(), 2);
		Assert.assertEquals(statics.getPutTimes(), 2);
		Assert.assertEquals(statics.getAccessTimes(), 3);
		Assert.assertEquals(statics.getHitTimes(), 2);
		Assert.assertTrue(statics.getLastAccesTime() > 0);
		Assert.assertTrue(statics.getLastPutTime() > 0);
		Assert.assertTrue(statics.getLastAccesTime() > 0);
		// wait expire
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
		}
		Assert.assertEquals(null, enCachePool.get("2"));
		Assert.assertEquals(null, enCachePool.get("1"));
	}

}