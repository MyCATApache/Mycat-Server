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
package org.opencloudb.net;

import java.nio.channels.CompletionHandler;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;

/**
 * @author mycat
 */
public final class AIOConnector implements SocketConnector,
		CompletionHandler<Void, BackendAIOConnection> {
	private static final Logger LOGGER = Logger.getLogger(AIOConnector.class);
	private static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

	public AIOConnector() {

	}

	@Override
	public void completed(Void result, BackendAIOConnection attachment) {
		finishConnect(attachment);
	}

	@Override
	public void failed(Throwable exc, BackendAIOConnection conn) {
		conn.onConnectFailed(exc);
	}

	private void finishConnect(BackendAIOConnection c) {
		try {
			if (c.finishConnect()) {
				c.setId(ID_GENERATOR.getId());
				NIOProcessor processor = MycatServer.getInstance()
						.nextProcessor();
				c.setProcessor(processor);
				c.register();
			}
		} catch (Exception e) {
			c.onConnectFailed(e);
			LOGGER.info("connect err " + e);
			c.close(e.toString());
		}
	}

	/**
	 * 后端连接ID生成器
	 * 
	 * @author mycat
	 */
	private static class ConnectIdGenerator {

		private static final long MAX_VALUE = Long.MAX_VALUE;

		private long connectId = 0L;
		private final Object lock = new Object();

		private long getId() {
			synchronized (lock) {
				if (connectId >= MAX_VALUE) {
					connectId = 0L;
				}
				return ++connectId;
			}
		}
	}

}