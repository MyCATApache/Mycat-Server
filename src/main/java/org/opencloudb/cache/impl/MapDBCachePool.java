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

import org.mapdb.HTreeMap;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheStatic;

public class MapDBCachePool implements CachePool {

	private final HTreeMap<Object, Object> htreeMap;
	private final CacheStatic cacheStati = new CacheStatic();
    private final long maxSize;
	public MapDBCachePool(HTreeMap<Object, Object> htreeMap,long maxSize) {
		this.htreeMap = htreeMap;
		this.maxSize=maxSize;
		cacheStati.setMaxSize(maxSize);
	}

	@Override
	public void putIfAbsent(Object key, Object value) {
		if (htreeMap.putIfAbsent(key, value) == null) {
			cacheStati.incPutTimes();
		}

	}

	@Override
	public Object get(Object key) {
		Object value = htreeMap.get(key);
		if (value != null) {
			cacheStati.incHitTimes();
			return value;
		} else {
			cacheStati.incAccessTimes();
			return null;
		}
	}

	@Override
	public void clearCache() {
		htreeMap.clear();
		cacheStati.reset();

	}

	@Override
	public CacheStatic getCacheStatic() {
		
		cacheStati.setItemSize(htreeMap.sizeLong());
		return cacheStati;
	}

	@Override
	public long getMaxSize() {
		return maxSize;
	}

}