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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 * 网络事件反应器
 * 
 * @author mycat
 */
public final class NIOReactor {
	
	private static final Logger LOGGER = Logger.getLogger(NIOReactor.class);
	private final String name;
	private final RW reactorR;

	public NIOReactor(String name) throws IOException {
		this.name = name;
		this.reactorR = new RW();
	}

	final void startup() {
		new Thread(reactorR, name + "-RW").start();
	}

	//NIOConnector和NIOAcceptor建立连接后，调用NIOReactor.postRegister进行注册
	final void postRegister(AbstractConnection c) {
		
		//放到队列中
		reactorR.registerQueue.offer(c);
		
		//然后再唤醒selector
		reactorR.selector.wakeup();
	}

	final Queue<AbstractConnection> getRegisterQueue() {
		return reactorR.registerQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

	//某一条链路
	private final class RW implements Runnable {
		
		private final Selector selector;
		private final ConcurrentLinkedQueue<AbstractConnection> registerQueue;
		private long reactCount;

		private RW() throws IOException {
			this.selector = Selector.open();
			this.registerQueue = new ConcurrentLinkedQueue<AbstractConnection>();
		}

		@Override
		public void run() {
			final Selector selector = this.selector;
			Set<SelectionKey> keys = null;
			
			for (;;) {
				
				++reactCount;
				
				try {
					
					//selector不断监听连接事件
					selector.select(500L);
					
					//
					register(selector);
					
					keys = selector.selectedKeys();
					for (SelectionKey key : keys) {
						AbstractConnection con = null;
						try {
							Object att = key.attachment();
							if (att != null) {
								con = (AbstractConnection) att;
								if (key.isValid() && key.isReadable()) {
									
									try {
										/**
										 * 读事件
										 * 
										 * 调用con.asynRead()函数，进行字节的读取
										 */
										con.asynRead();
										
									} catch (IOException e) {
                                        con.close("program err:" + e.toString());
										continue;
									} catch (Exception e) {
										LOGGER.debug("caught err:", e);
										con.close("program err:" + e.toString());
										continue;
									}
								}
								
								/**
								 * 写事件
								 * 
								 * 调用AbstractConnection 的 doNextWriteCheck() 进行处理，在doNextWriteCheck()中，
								 * 又调用 NIOSocketWR.doNextWriteCheck()进行处理
								 */
								if (key.isValid() && key.isWritable()) {
									con.doNextWriteCheck();
								}
							} else {
								key.cancel();
							}
                        } catch (CancelledKeyException e) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(con + " socket key canceled");
                            }
                        } catch (Exception e) {
                            LOGGER.warn(con + " " + e);
                        }
					}
				} catch (Exception e) {
					LOGGER.warn(name, e);
				} finally {
					if (keys != null) {
						keys.clear();
					}
				}
			}
		}

		/**
		 * 处理等待 R/W的队列
		 * 
		 * @param selector
		 */
		private void register(Selector selector) {
			
			AbstractConnection c = null;
			if (registerQueue.isEmpty()) {
				return;
			}
			
			while ((c = registerQueue.poll()) != null) {
				try {
					//这儿通过Connection 中的 NIOSocketWR 注册OP_READ事件
					((NIOSocketWR) c.getSocketWR()).register(selector);
					
					c.register();
				} catch (Exception e) {
					c.close("register err" + e.toString());
				}
			}
		}

	}

}