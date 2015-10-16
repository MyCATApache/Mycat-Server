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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;

/**
 * 处理 与MySql Server 建立连接
 * 
 * @author mycat
 */
public final class NIOConnector extends Thread implements SocketConnector {
	
	private static final Logger LOGGER = Logger.getLogger(NIOConnector.class);
	
	public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

	private final String name;
	
	//事件选择器
	private final Selector selector;
	
	//需要建立连接的对象，临时放在这个队列里
	private final BlockingQueue<AbstractConnection> connectQueue;
	private long connectCount;
	
	//当连接建立后，从reactorPool中分配一个NIOReactor，处理Read和Write事件
	private final NIOReactorPool reactorPool;

	public NIOConnector(String name, NIOReactorPool reactorPool) throws IOException {
		
		super.setName(name);
		this.name = name;
		this.selector = Selector.open();
		this.reactorPool = reactorPool;
		this.connectQueue = new LinkedBlockingQueue<AbstractConnection>();
	}

	public long getConnectCount() {
		return connectCount;
	}

	/**
	 *  把需要建立的连接放到 connectQueue 队列中，然后再唤醒selector，
	 *  
	 *  postConnect 是在新建连接或者心跳时被XXXXConnectionFactory触发的
	 */
	public void postConnect(AbstractConnection c) {
		connectQueue.offer(c);
		selector.wakeup();
	}

	@Override
	public void run() {
		
		final Selector tSelector = this.selector;
		
		//无限循环
		for (;;) {
			++connectCount;
			try {				
				//阻塞，等待有事件发生唤醒
				tSelector.select(1000L);
			    
				//建立连接
			    connect(tSelector);
				
				Set<SelectionKey> keys = tSelector.selectedKeys();
				try {
					for (SelectionKey key : keys) {
						Object att = key.attachment();						
						if (att != null && key.isValid() && key.isConnectable()) {
							finishConnect(key, att);
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
				
			} catch (Exception e) {
				LOGGER.warn(name, e);
			}
		}
	}

	//处理postConnect函数操作的connectQueue队列
	private void connect(Selector selector) {
		
		AbstractConnection c = null;
		
		//判断connectQueue中是否新的连接请求
		while ((c = connectQueue.poll()) != null) {
			try {
				//建立一个SocketChannel
				SocketChannel channel = (SocketChannel) c.getChannel();
				
				//在selector中进行注册OP_CONNECT
				channel.register(selector, SelectionKey.OP_CONNECT, c);
				
				//发起SocketChannel.connect()操作
				channel.connect(new InetSocketAddress(c.host, c.port));
			} catch (Exception e) {
				c.close(e.toString());
			}
		}
	}

	//只注册了OP_CONNECT事件，所以只对OP_CONNECT事件进行处理
	private void finishConnect(SelectionKey key, Object att) {
		
		BackendAIOConnection c = (BackendAIOConnection) att;
		try {
			
			if ( finishConnect(c, (SocketChannel) c.channel) ) {				
				clearSelectionKey(key);
				c.setId(ID_GENERATOR.getId());
				
				NIOProcessor processor = MycatServer.getInstance().nextProcessor();
				c.setProcessor(processor);
				
				// 当连接建立完毕后，分配一个NIOReactor，处理后续的Read和Write事件
				NIOReactor reactor = reactorPool.getNextReactor();
				reactor.postRegister(c);
			}
			
		} catch (Exception e) {
			clearSelectionKey(key);
            c.close(e.toString());
			c.onConnectFailed(e);

		}
	}

	//完成连接
	private boolean finishConnect(AbstractConnection c, SocketChannel channel)
			throws IOException {
		
		if (channel.isConnectionPending()) {
			channel.finishConnect();			
			c.setLocalPort(channel.socket().getLocalPort());
			
			return true;
		} else {
			return false;
		}
	}

	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}

	/**
	 * 后端连接ID生成器
	 * 
	 * @author mycat
	 */
	public static class ConnectIdGenerator {

		private static final long MAX_VALUE = Long.MAX_VALUE;

		private long connectId = 0L;
		private final Object lock = new Object();

		public long getId() {
			synchronized (lock) {
				if (connectId >= MAX_VALUE) {
					connectId = 0L;
				}
				return ++connectId;
			}
		}
	}

}