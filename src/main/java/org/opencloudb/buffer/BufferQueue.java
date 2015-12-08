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
package org.opencloudb.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author mycat
 */
public final class BufferQueue {
	private final long total;
	private final LinkedList<ByteBuffer> items = new LinkedList<ByteBuffer>();

	public BufferQueue(long capacity) {
		this.total = capacity;
	}

	/**
	 * used for statics
	 * 
	 * @return
	 */
	public int snapshotSize() {
		return this.items.size();
	}

	public Collection<ByteBuffer> removeItems(long count) {

		List<ByteBuffer> removed = new ArrayList<ByteBuffer>();
		Iterator<ByteBuffer> itor = items.iterator();
		while (itor.hasNext()) {
			removed.add(itor.next());
			itor.remove();
			if (removed.size() >= count) {
				break;
			}
		}
		return removed;
	}

	/**
	 * 
	 * @param buffer
	 * @throws InterruptedException
	 */
	public void put(ByteBuffer buffer) {
		this.items.offer(buffer);
		if (items.size() > total) {
			throw new java.lang.RuntimeException(
					"bufferQueue size exceeded ,maybe sql returned too many records ,cursize:"
							+ items.size());

		}
	}

	public ByteBuffer poll() {
		return items.poll();
	}

	public boolean isEmpty() {
		return items.isEmpty();
	}

}