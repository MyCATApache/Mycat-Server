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

/**
 * cache static information
 * 
 * @author wuzhih
 * 
 */
public class CacheStatic {
	private long maxSize;
	private long memorySize;
	private long itemSize;
	private long accessTimes;
	private long putTimes;
	private long hitTimes;
	private long lastAccesTime;
	private long lastPutTime;

	public long getMemorySize() {
		return memorySize;
	}

	public void setMemorySize(long memorySize) {
		this.memorySize = memorySize;
	}

	public long getItemSize() {
		return itemSize;
	}

	public void setItemSize(long itemSize) {
		this.itemSize = itemSize;
	}

	public long getAccessTimes() {
		return accessTimes;
	}

	public void setAccessTimes(long accessTimes) {
		this.accessTimes = accessTimes;
	}

	public long getHitTimes() {
		return hitTimes;
	}

	public void setHitTimes(long hitTimes) {
		this.hitTimes = hitTimes;
	}

	public long getLastAccesTime() {
		return lastAccesTime;
	}

	public void setLastAccesTime(long lastAccesTime) {
		this.lastAccesTime = lastAccesTime;
	}

	public long getPutTimes() {
		return putTimes;
	}

	public void setPutTimes(long putTimes) {
		this.putTimes = putTimes;
	}

	public void incAccessTimes() {
		this.accessTimes++;
		this.lastAccesTime = System.currentTimeMillis();
	}

	public void incHitTimes() {
		this.hitTimes++;
		this.accessTimes++;
		this.lastAccesTime = System.currentTimeMillis();
	}

	public void incPutTimes() {
		this.putTimes++;
		this.lastPutTime = System.currentTimeMillis();
	}

	public long getLastPutTime() {
		return lastPutTime;
	}

	public void setLastPutTime(long lastPutTime) {
		this.lastPutTime = lastPutTime;
	}

	public long getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(long maxSize) {
		this.maxSize = maxSize;
	}

	public void reset() {
		this.accessTimes = 0;
		this.hitTimes = 0;
		this.itemSize = 0;
		this.lastAccesTime = 0;
		this.lastPutTime = 0;
		this.memorySize = 0;
		this.putTimes = 0;

	}

	@Override
	public String toString() {
		return "CacheStatic [memorySize=" + memorySize + ", itemSize="
				+ itemSize + ", accessTimes=" + accessTimes + ", putTimes="
				+ putTimes + ", hitTimes=" + hitTimes + ", lastAccesTime="
				+ lastAccesTime + ", lastPutTime=" + lastPutTime + "]";
	}

}