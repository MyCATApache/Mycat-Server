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
package io.mycat;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.mycat.cache.CacheStatic;
import io.mycat.cache.LayerCachePool;

public class SimpleCachePool implements LayerCachePool {
	private HashMap<Object, Object> cacheMap;

	public SimpleCachePool() {
		long MAX_CACHE_SIZE = getMaxSize();
		float factor = 0.75f;
		int capacity = (int)Math.ceil(MAX_CACHE_SIZE / factor) + 1;
		cacheMap =  new LinkedHashMap<Object, Object>(capacity,  factor, true) {
			@Override
			protected boolean removeEldestEntry(Map.Entry<Object, Object> eldest) {
				return size() > MAX_CACHE_SIZE;
			}};
	}

	@Override
	public void putIfAbsent(Object key, Object value) {
		cacheMap.put(key, value);
	}

	@Override
	public Object get(Object key) {
		return cacheMap.get(key);
	}

	@Override
	public void clearCache() {
		cacheMap.clear();

	}

	@Override
	public CacheStatic getCacheStatic() {
		return null;
	}

	@Override
	public void putIfAbsent(String primaryKey, Object secondKey, Object value) {
		putIfAbsent(primaryKey+"_"+secondKey,value);
		
	}

	@Override
	public Object get(String primaryKey, Object secondKey) {
		return get(primaryKey+"_"+secondKey);
	}

	@Override
	public Map<String, CacheStatic> getAllCacheStatic() {

		return null;
	}

	@Override
	public void clearCache(String cacheName) {
		if (cacheName != null) {
			cacheMap.remove(cacheName);
		}
	}

	@Override
	public long getMaxSize() {
		return 100;
	}
};