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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.net.factory.FrontendConnectionFactory;

/**
 * 处理 与  客户端 建立连接
 * 
 * @author mycat
 */
public final class NIOAcceptor extends Thread  implements SocketAcceptor{
	
	private static final Logger LOGGER = Logger.getLogger(NIOAcceptor.class);
	
	private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

	private final int port;
	
	//事件选择器
	private final Selector selector;
	
	//监听新进来的TCP连接的通道
	private final ServerSocketChannel serverChannel;
	private final FrontendConnectionFactory factory;
	private long acceptCount;
	
	//当连接建立后，从reactorPool中分配一个NIOReactor，处理Read和Write事件
	private final NIOReactorPool reactorPool;
	

	//监听通道在此构造函数里启动
	public NIOAcceptor(String name, String bindIp, int port,
			FrontendConnectionFactory factory, NIOReactorPool reactorPool)
			throws IOException {
		
		super.setName(name);
		
		this.port = port;
		this.selector = Selector.open();
		this.serverChannel = ServerSocketChannel.open();
		this.serverChannel.configureBlocking(false);
		
		/** 设置TCP属性 */
		serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
		
		// 指定等待连接的队列大小 backlog=100
		serverChannel.bind(new InetSocketAddress(bindIp, port), 100);
		this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		
		this.factory = factory;
		this.reactorPool = reactorPool;
	}

	public int getPort() {
		return port;
	}

	public long getAcceptCount() {
		return acceptCount;
	}

	@Override
	public void run() {
		
		final Selector tSelector = this.selector;
		
		//无限循环
		for (;;) {
			
			++acceptCount;
			
			try {
				
				//selector不断监听连接事件, 然后在accept()函数中对事件进行处理
			    tSelector.select(1000L);			    
				Set<SelectionKey> keys = tSelector.selectedKeys();
				try {
					for (SelectionKey key : keys) {
						if (key.isValid() && key.isAcceptable()) {
							
							accept();
							
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
			} catch (Exception e) {
				LOGGER.warn(getName(), e);
			}
		}
	}

	//只注册了OP_ACCEPT事件，所以只对OP_ACCEPT事件进行处理
	private void accept() {
		
		SocketChannel channel = null;
		try {
			channel = serverChannel.accept();
			channel.configureBlocking(false);
			
			FrontendConnection c = factory.make(channel);
			c.setAccepted(true);
			c.setId(ID_GENERATOR.getId());
			
			NIOProcessor processor = (NIOProcessor) MycatServer.getInstance().nextProcessor();
			c.setProcessor(processor);
			
			//从reactorPool中获得一个NIOReactor, 后续的Read和Write事件就交给NIOReactor处理了
			NIOReactor reactor = reactorPool.getNextReactor();
			reactor.postRegister(c);

		} catch (Exception e) {
	        LOGGER.warn(getName(), e);
			closeChannel(channel);
		}
	}

	private static void closeChannel(SocketChannel channel) {
		
		if (channel == null) {
			return;
		}
		
		Socket socket = channel.socket();
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
		       LOGGER.error("closeChannelError", e);
			}
		}
		
		try {
			channel.close();
		} catch (IOException e) {
            LOGGER.error("closeChannelError", e);
		}
	}

	/**
	 * 前端连接ID生成器
	 * 
	 * @author mycat
	 */
	private static class AcceptIdGenerator {

		private static final long MAX_VALUE = 0xffffffffL;

		private long acceptId = 0L;
		private final Object lock = new Object();

		private long getId() {
			synchronized (lock) {
				if (acceptId >= MAX_VALUE) {
					acceptId = 0L;
				}
				return ++acceptId;
			}
		}
	}

}