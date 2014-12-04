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
package org.opencloudb.cache.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;

import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CachePoolFactory;

public class EnchachePooFactory extends CachePoolFactory {

	@Override
	public CachePool createCachePool(String poolName, int cacheSize,
			int expiredSeconds) {
		CacheManager cacheManager = CacheManager.create();
		Cache enCache = cacheManager.getCache(poolName);
		if (enCache == null) {

			CacheConfiguration cacheConf = cacheManager.getConfiguration()
					.getDefaultCacheConfiguration().clone();
			cacheConf.setName(poolName);
			if (cacheConf.getMaxEntriesLocalHeap() != 0) {
				cacheConf.setMaxEntriesLocalHeap(cacheSize);
			} else {
				cacheConf.setMaxBytesLocalHeap(String.valueOf(cacheSize));
			}
			cacheConf.setTimeToIdleSeconds(expiredSeconds);
			Cache cache = new Cache(cacheConf);
			cacheManager.addCache(cache);
			return new EnchachePool(poolName,cache,cacheSize);
		} else {
			return new EnchachePool(poolName,enCache,cacheSize);
		}
	}

}