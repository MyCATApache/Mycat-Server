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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

/**
 * cache service for other component default using memory cache encache
 * 
 * @author wuzhih
 * 
 */
public class CacheService {
	private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

	private final Map<String, CachePoolFactory> poolFactorys = new HashMap<String, CachePoolFactory>();
	private final Map<String, CachePool> allPools = new HashMap<String, CachePool>();

	public CacheService() {

		// load cache pool defined
		try {
			init();
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		}

	}
	public Map<String, CachePool> getAllCachePools()
	{
		return this.allPools;
	}

	private void init() throws Exception {
		Properties props = new Properties();
		props.load(CacheService.class
				.getResourceAsStream("/cacheservice.properties"));
		final String poolFactoryPref = "factory.";
		final String poolKeyPref = "pool.";
		final String layedPoolKeyPref = "layedpool.";
		String[] keys = props.keySet().toArray(new String[0]);
		Arrays.sort(keys);
		for (String key : keys) {

			if (key.startsWith(poolFactoryPref)) {
				createPoolFactory(key.substring(poolFactoryPref.length()),
						(String) props.get(key));
			} else if (key.startsWith(poolKeyPref)) {
				String cacheName = key.substring(poolKeyPref.length());
				String value = (String) props.get(key);
				String[] valueItems = value.split(",");
				if (valueItems.length < 3) {
					throw new java.lang.IllegalArgumentException(
							"invalid cache config ,key:" + key + " value:"
									+ value);
				}
				String type = valueItems[0];
				int size = Integer.parseInt(valueItems[1]);
				int timeOut = Integer.parseInt(valueItems[2]);
				createPool(cacheName, type, size, timeOut);
			} else if (key.startsWith(layedPoolKeyPref)) {
				String cacheName = key.substring(layedPoolKeyPref.length());
				String value = (String) props.get(key);
				String[] valueItems = value.split(",");
				int index = cacheName.indexOf(".");
				if (index < 0) {// root layer
					String type = valueItems[0];
					int size = Integer.valueOf(valueItems[1]);
					int timeOut = Integer.valueOf(valueItems[2]);
					createLayeredPool(cacheName, type, size, timeOut);
				} else {
					// root layers' children
					String parent = cacheName.substring(0, index);
					String child = cacheName.substring(index + 1);
					CachePool pool = this.allPools.get(parent);
					if (pool == null || !(pool instanceof LayerCachePool)) {
						throw new java.lang.IllegalArgumentException(
								"parent pool not exists or not layered cache pool:"
										+ parent + " the child cache is:"
										+ child);
					}

					int size = Integer.valueOf(valueItems[0]);
					int timeOut = Integer.valueOf(valueItems[1]);
					((DefaultLayedCachePool) pool).createChildCache(child,
							size, timeOut);
				}
			}
		}
	}

	private void createLayeredPool(String cacheName, String type, int size,
			int expireSeconds) {
		checkExists(cacheName);
		logger.info("create layer cache pool " + cacheName + " of type " + type
				+ " ,default cache size " + size + " ,default expire seconds"
				+ expireSeconds);
		DefaultLayedCachePool layerdPool = new DefaultLayedCachePool(cacheName,
				this.getCacheFact(type), size, expireSeconds);
		this.allPools.put(cacheName, layerdPool);

	}

	private void checkExists(String poolName) {
		if (allPools.containsKey(poolName)) {
			throw new java.lang.IllegalArgumentException(
					"duplicate cache pool name: " + poolName);
		}
	}

	private void createPoolFactory(String factryType, String factryClassName)
			throws Exception {
		CachePoolFactory factry = (CachePoolFactory) Class.forName(
				factryClassName).newInstance();
		poolFactorys.put(factryType, factry);

	}

	private void createPool(String poolName, String type, int cacheSize,
			int expireSeconds) {
		checkExists(poolName);
		CachePoolFactory cacheFact = getCacheFact(type);
		CachePool cachePool = cacheFact.createCachePool(poolName, cacheSize,
				expireSeconds);
		allPools.put(poolName, cachePool);

	}

	private CachePoolFactory getCacheFact(String type) {
		CachePoolFactory facty = this.poolFactorys.get(type);
		if (facty == null) {
			throw new RuntimeException("CachePoolFactory not defined for type:"
					+ type);
		}
		return facty;
	}

	/**
	 * get cache pool by name ,caller should cache result
	 * 
	 * @param poolName
	 * @return CachePool
	 */
	public CachePool getCachePool(String poolName) {
		CachePool pool = allPools.get(poolName);
		if (pool == null) {
			throw new IllegalArgumentException("can't find cache pool:"
					+ poolName);
		} else {
			return pool;
		}

	}

	public void clearCache() {

		logger.info("clear all cache pool ");
		for (CachePool pool : allPools.values()) {

			pool.clearCache();
		}

	}

}