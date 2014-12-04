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

import org.junit.Test;
import org.opencloudb.cache.impl.EnchachePooFactory;

public class  DefaultLayedCachePoolTest {

	private static DefaultLayedCachePool layedCachePool;
	static {
		
		layedCachePool=new DefaultLayedCachePool("defaultLayedPool",new EnchachePooFactory(),1000,1);
		
	}

	@Test
	public void testBasic() {
		layedCachePool.putIfAbsent("2", "dn2");
		layedCachePool.putIfAbsent("1", "dn1");

		layedCachePool.putIfAbsent("company", 1, "dn1");
		layedCachePool.putIfAbsent("company", 2, "dn2");

		layedCachePool.putIfAbsent("goods", "1", "dn1");
		layedCachePool.putIfAbsent("goods", "2", "dn2");

		Assert.assertEquals("dn2", layedCachePool.get("2"));
		Assert.assertEquals("dn1", layedCachePool.get("1"));
		Assert.assertEquals(null, layedCachePool.get("3"));

		Assert.assertEquals("dn1", layedCachePool.get("company", 1));
		Assert.assertEquals("dn2", layedCachePool.get("company", 2));
		Assert.assertEquals(null, layedCachePool.get("company", 3));

		Assert.assertEquals("dn1", layedCachePool.get("goods", "1"));
		Assert.assertEquals("dn2", layedCachePool.get("goods", "2"));
		Assert.assertEquals(null, layedCachePool.get("goods", 3));
		CacheStatic statics = layedCachePool.getCacheStatic();
		Assert.assertEquals(statics.getItemSize(), 6);
		Assert.assertEquals(statics.getPutTimes(), 6);
		Assert.assertEquals(statics.getAccessTimes(), 9);
		Assert.assertEquals(statics.getHitTimes(), 6);
		Assert.assertTrue(statics.getLastAccesTime() > 0);
		Assert.assertTrue(statics.getLastPutTime() > 0);
		Assert.assertTrue(statics.getLastAccesTime() > 0);
		// wait expire
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		Assert.assertEquals(null, layedCachePool.get("2"));
		Assert.assertEquals(null, layedCachePool.get("1"));
		Assert.assertEquals(null, layedCachePool.get("goods", "2"));
		Assert.assertEquals(null, layedCachePool.get("company", 2));
	}

}