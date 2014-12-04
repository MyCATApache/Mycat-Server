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
package org.opencloudb.heartbeat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

import org.opencloudb.MycatServer;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.net.NIOConnector;
import org.opencloudb.net.factory.BackendConnectionFactory;

/**
 * @author mycat
 */
public class MySQLDetectorFactory extends BackendConnectionFactory {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MySQLDetector make(MySQLHeartbeat heartbeat) throws IOException {
		DBHostConfig dsc = heartbeat.getSource().getConfig();
		NetworkChannel channel = openSocketChannel(MycatServer.getInstance().isAIO());
		MySQLDetector detector = new MySQLDetector(channel);
		 MycatServer.getInstance().getConfig().setSocketParams(detector, false);
		detector.setHost(dsc.getIp());
		detector.setPort(dsc.getPort());
		detector.setUser(dsc.getUser());
		detector.setPassword(dsc.getPassword());
		// detector.setSchema(dsc.getDatabase());
		detector.setHeartbeatTimeout(heartbeat.getHeartbeatTimeout());
		detector.setHeartbeat(heartbeat);
		if (channel instanceof AsynchronousSocketChannel) {
			((AsynchronousSocketChannel) channel).connect(
					new InetSocketAddress(dsc.getIp(), dsc.getPort()),
					detector, (CompletionHandler) MycatServer.getInstance()
							.getConnector());
		} else {
			// c.setPacketHeaderSize(packetHeaderSize);
			// c.setMaxPacketSize(maxPacketSize);
			// c.setWriteQueue(new BufferQueue(writeQueueCapcity));
			// c.setIdleTimeout(idleTimeout);
			// c.setConnector(connector);
			((NIOConnector) MycatServer.getInstance().getConnector())
					.postConnect(detector);

		}
		return detector;
	}

}