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
package io.mycat.net;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

/**
 * 网络事件反应器
 * 
 * <p>
 * Catch exceptions such as OOM so that the reactor can keep running for response client!
 * </p>
 * @since 2016-03-30
 * 
 * @author mycat, Uncle-pan
 * 
 */
public final class NIOReactor {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOReactor.class);
	private final String name;
	private final RW reactorR;

	public NIOReactor(String name) throws IOException {
		this.name = name;
		this.reactorR = new RW();
	}

	final void startup() {
		new Thread(reactorR, name + "-RW").start();
	}

	final void postRegister(AbstractConnection c) {
		reactorR.registerQueue.offer(c);
		reactorR.selector.wakeup();
	}

	final Queue<AbstractConnection> getRegisterQueue() {
		return reactorR.registerQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

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
					selector.select(500L);
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
										con.asynRead();
									} catch (IOException e) {
                                        con.close("program err:" + e.toString());
										continue;
									} catch (Exception e) {
										LOGGER.warn("caught err:", e);
										con.close("program err:" + e.toString());
										continue;
									}
								}
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
                        } catch (final Throwable e){
                        	// Catch exceptions such as OOM and close connection if exists
                        	//so that the reactor can keep running!
                        	// @author Uncle-pan
                        	// @since 2016-03-30
                        	if(con != null){
                        		con.close("Bad: "+e);
                        	}
                        	LOGGER.error("caught err: ", e);
                        	continue;
                        }
					}
				} catch (Exception e) {
					LOGGER.warn(name, e);
				} catch (final Throwable e){
					// Catch exceptions such as OOM so that the reactor can keep running!
                	// @author Uncle-pan
                	// @since 2016-03-30
					LOGGER.error("caught err: ", e);
				} finally {
					if (keys != null) {
						keys.clear();
					}

				}
			}
		}

		private void register(Selector selector) {
			AbstractConnection c = null;
			if (registerQueue.isEmpty()) {
				return;
			}
			while ((c = registerQueue.poll()) != null) {
				try {
					((NIOSocketWR) c.getSocketWR()).register(selector);
					c.register();
				} catch (Exception e) {
					c.close("register err" + e.toString());
				}
			}
		}

	}

}
