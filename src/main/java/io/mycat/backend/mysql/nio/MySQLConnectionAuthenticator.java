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

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.backend.mysql.SecurityUtil;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.Capabilities;
import io.mycat.net.ConnectionException;
import io.mycat.net.NIOHandler;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.HandshakePacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.Reply323Packet;

/**
 * MySQL 验证处理器
 * 
 * @author mycat
 */
public class MySQLConnectionAuthenticator implements NIOHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MySQLConnectionAuthenticator.class);
	private final MySQLConnection source;
	private final ResponseHandler listener;

	public MySQLConnectionAuthenticator(MySQLConnection source,
			ResponseHandler listener) {
		this.source = source;
		this.listener = listener;
	}

	public void connectionError(MySQLConnection source, Throwable e) {
		listener.connectionError(e, source);
	}

	@Override
	public void handle(byte[] data) {
		try {
			switch (data[4]) {
			case OkPacket.FIELD_COUNT:
				HandshakePacket packet = source.getHandshake();
				if (packet == null) {
					processHandShakePacket(data);
					// 发送认证数据包
					source.authenticate();
					break;
				}
				// 处理认证结果
				source.setHandler(new MySQLConnectionHandler(source));
				source.setAuthenticated(true);
				boolean clientCompress = Capabilities.CLIENT_COMPRESS==(Capabilities.CLIENT_COMPRESS & packet.serverCapabilities);
				boolean usingCompress= MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
				if(clientCompress&&usingCompress)
				{
					source.setSupportCompress(true);
				}
				if (listener != null) {
					listener.connectionAcquired(source);
				}
				break;
			case ErrorPacket.FIELD_COUNT:
				ErrorPacket err = new ErrorPacket();
				err.read(data);
				String errMsg = new String(err.message);
				LOGGER.warn("can't connect to mysql server ,errmsg:"+errMsg+" "+source);
				//source.close(errMsg);
				throw new ConnectionException(err.errno, errMsg);

			case EOFPacket.FIELD_COUNT:
				auth323(data[3]);
				break;
			default:
				packet = source.getHandshake();
				if (packet == null) {
					processHandShakePacket(data);
					// 发送认证数据包
					source.authenticate();
					break;
				} else {
					throw new RuntimeException("Unknown Packet!");
				}

			}

		} catch (RuntimeException e) {
			if (listener != null) {
				listener.connectionError(e, source);
				return;
			}
			throw e;
		}
	}

	private void processHandShakePacket(byte[] data) {
		// 设置握手数据包
		HandshakePacket packet= new HandshakePacket();
		packet.read(data);
		source.setHandshake(packet);
		source.setThreadId(packet.threadId);

		// 设置字符集编码
		int charsetIndex = (packet.serverCharsetIndex & 0xff);
		String charset = CharsetUtil.getCharset(charsetIndex);
		if (charset != null) {
			source.setCharset(charset);
		} else {
			throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
		}
	}

	private void auth323(byte packetId) {
		// 发送323响应认证数据包
		Reply323Packet r323 = new Reply323Packet();
		r323.packetId = ++packetId;
		String pass = source.getPassword();
		if (pass != null && pass.length() > 0) {
			byte[] seed = source.getHandshake().seed;
			r323.seed = SecurityUtil.scramble323(pass, new String(seed))
					.getBytes();
		}
		r323.write(source);
	}

}