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
package org.opencloudb.net.handler;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencloudb.net.NIOHandler;

/**
 * @author mycat
 */
public abstract class BackendAsyncHandler implements NIOHandler {

	protected final ConcurrentLinkedQueue<byte[]> dataQueue = new ConcurrentLinkedQueue<byte[]>();
	protected final AtomicBoolean isHandling = new AtomicBoolean(false);

	protected void offerData(byte[] data, Executor executor) {
		handleData(data);
//		if (dataQueue.offer(data)) {
//			handleQueue(executor);
//		} else {
//			offerDataError();
//		}
	}

	protected abstract void offerDataError();

	protected abstract void handleData(byte[] data);

	protected abstract void handleDataError(Throwable t);

	protected void handleQueue(final Executor executor) {
		if (isHandling.compareAndSet(false, true)) {
			// asynchronize execute
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						byte[] data = null;
						while ((data = dataQueue.poll()) != null) {
							handleData(data);
						}
					} catch (Throwable t) {
						handleDataError(t);
					} finally {
						isHandling.set(false);
						if (!dataQueue.isEmpty()) {
							handleQueue(executor);
						}
					}
				}
			});

		}
	}
}