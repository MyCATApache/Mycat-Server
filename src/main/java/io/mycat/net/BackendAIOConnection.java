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
import java.net.InetSocketAddress;
import java.nio.channels.NetworkChannel;

import io.mycat.backend.BackendConnection;

/**
 * @author mycat
 */
public abstract class BackendAIOConnection extends AbstractConnection implements
		BackendConnection {

	
	
	protected boolean isFinishConnect;

	public BackendAIOConnection(NetworkChannel channel) {
		super(channel);
	}

	public void register() throws IOException {
		this.asynRead();
	}


	public void setHost(String host) {
		this.host = host;
	}


	public void setPort(int port) {
		this.port = port;
	}

	

	
	public void discardClose(String reason){
		//跨节点处理,中断后端连接时关闭
	}
	public abstract void onConnectFailed(Throwable e);

	public boolean finishConnect() throws IOException {
		localPort = ((InetSocketAddress) channel.getLocalAddress()).getPort();
		isFinishConnect = true;
		return true;
	}

	public void setProcessor(NIOProcessor processor) {
		super.setProcessor(processor);
		processor.addBackend(this);
	}

	@Override
	public String toString() {
		return "BackendConnection [id=" + id + ", host=" + host + ", port="
				+ port + ", localPort=" + localPort + "]";
	}
}