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
package io.mycat.backend.mysql.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.model.DBHostConfig;
import io.mycat.net.NIOConnector;
import io.mycat.net.factory.BackendConnectionFactory;

/**
 * @author mycat
 */
public class MySQLConnectionFactory extends BackendConnectionFactory {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MySQLConnection make(MySQLDataSource pool, ResponseHandler handler,
			String schema) throws IOException {

		DBHostConfig dsc = pool.getConfig();
		NetworkChannel channel = openSocketChannel(MycatServer.getInstance()
				.isAIO());

		MySQLConnection c = new MySQLConnection(channel, pool.isReadNode());
		MycatServer.getInstance().getConfig().setSocketParams(c, false);
		c.setHost(dsc.getIp());
		c.setPort(dsc.getPort());
		c.setUser(dsc.getUser());
		c.setPassword(dsc.getPassword());
		c.setSchema(schema);
		c.setHandler(new MySQLConnectionAuthenticator(c, handler));
		c.setPool(pool);
		c.setIdleTimeout(pool.getConfig().getIdleTimeout());
		if (channel instanceof AsynchronousSocketChannel) {
			((AsynchronousSocketChannel) channel).connect(
					new InetSocketAddress(dsc.getIp(), dsc.getPort()), c,
					(CompletionHandler) MycatServer.getInstance()
							.getConnector());
		} else {
			((NIOConnector) MycatServer.getInstance().getConnector())
					.postConnect(c);

		}
		return c;
	}

}