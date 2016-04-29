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
package org.opencloudb.mysql.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

import org.opencloudb.MycatServer;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.NIOConnector;
import org.opencloudb.net.factory.BackendConnectionFactory;

/**
 * @author mycat
 */
public class MySQLConnectionFactory extends BackendConnectionFactory {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MySQLConnection make(MySQLDataSource pool, ResponseHandler handler,
			String schema) throws IOException {
		//DBHost配置
		DBHostConfig dsc = pool.getConfig();
        //根据是否为NIO返回SocketChannel或者AIO的AsynchronousSocketChannel
		NetworkChannel channel = openSocketChannel(MycatServer.getInstance()
				.isAIO());
        //新建MySQLConnection
		MySQLConnection c = new MySQLConnection(channel, pool.isReadNode());
        //根据配置初始化MySQLConnection
		MycatServer.getInstance().getConfig().setSocketParams(c, false);
		c.setHost(dsc.getIp());
		c.setPort(dsc.getPort());
		c.setUser(dsc.getUser());
		c.setPassword(dsc.getPassword());
		c.setSchema(schema);
        //目前实际连接还未建立，handler为MySQL连接认证MySQLConnectionAuthenticator，传入的handler为后端连接处理器ResponseHandler
		c.setHandler(new MySQLConnectionAuthenticator(c, handler));
		c.setPool(pool);
		c.setIdleTimeout(pool.getConfig().getIdleTimeout());
        //AIO和NIO连接方式建立实际的MySQL连接
		if (channel instanceof AsynchronousSocketChannel) {
			((AsynchronousSocketChannel) channel).connect(
					new InetSocketAddress(dsc.getIp(), dsc.getPort()), c,
					(CompletionHandler) MycatServer.getInstance()
							.getConnector());
		} else {
            //通过NIOConnector建立连接
			((NIOConnector) MycatServer.getInstance().getConnector())
					.postConnect(c);

		}
		return c;
	}

}