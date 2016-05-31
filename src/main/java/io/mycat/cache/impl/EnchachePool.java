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
package io.mycat.cache.impl;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.cache.CachePool;
import io.mycat.cache.CacheStatic;

/**
 * ehcache based cache pool
 * 
 * @author wuzhih
 * 
 */
public class EnchachePool implements CachePool {
	private static final Logger LOGGER = LoggerFactory.getLogger(EnchachePool.class);
	private final Cache enCache;
	private final CacheStatic cacheStati = new CacheStatic();
    private final String name;
    private final long maxSize;
	public EnchachePool(String name,Cache enCache,long maxSize) {
		this.enCache = enCache;
		this.name=name;
		this.maxSize=maxSize;
		cacheStati.setMaxSize(this.getMaxSize());

	}

	@Override
	public void putIfAbsent(Object key, Object value) {
		Element el = new Element(key, value);
		if (enCache.putIfAbsent(el) == null) {
			cacheStati.incPutTimes();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(name+" add cache ,key:" + key + " value:" + value);
			}
		}

	}

	@Override
	public Object get(Object key) {
		Element cacheEl = enCache.get(key);
		if (cacheEl != null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(name+" hit cache ,key:" + key);
			}
			cacheStati.incHitTimes();
			return cacheEl.getObjectValue();
		} else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(name+"  miss cache ,key:" + key);
			}
			cacheStati.incAccessTimes();
			return null;
		}
	}

	@Override
	public void clearCache() {
		LOGGER.info("clear cache "+name);
		enCache.removeAll();
		enCache.clearStatistics();
		cacheStati.reset();
		cacheStati.setMemorySize(enCache.getMemoryStoreSize());

	}

	@Override
	public CacheStatic getCacheStatic() {
		
		cacheStati.setItemSize(enCache.getSize());
		return cacheStati;
	}

	@Override
	public long getMaxSize() {
		return maxSize;
	}

}