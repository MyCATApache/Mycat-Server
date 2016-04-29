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

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.Capabilities;
import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.mysql.SecurityUtil;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.ConnectionException;
import org.opencloudb.net.NIOHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.HandshakePacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.Reply323Packet;

/**
 * MySQL 验证处理器
 * 
 * @author mycat
 */
public class MySQLConnectionAuthenticator implements NIOHandler {
	private static final Logger LOGGER = Logger
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

	/**
	 * MySQL 4.1版本之前是MySQL323加密,MySQL 4.1和之后的版本都是MySQLSHA1加密，在MySQL5.5的版本之后可以客户端插件式加密
	 * @see @http://dev.mysql.com/doc/internals/en/determining-authentication-method.html
	 */
	@Override
	public void handle(byte[] data) {
		try {
			switch (data[4]) {
			//如果是OkPacket，检查是否认证成功
			case OkPacket.FIELD_COUNT:
				HandshakePacket packet = source.getHandshake();
				if (packet == null) {
					//如果为null，证明链接第一次建立，处理
					processHandShakePacket(data);
					// 发送认证数据包
					source.authenticate();
					break;
				}
				// 如果packet不为null，处理认证结果
				//首先将连接设为已验证并将handler改为MySQLConnectionHandler
				source.setHandler(new MySQLConnectionHandler(source));
				source.setAuthenticated(true);
				//判断是否用了压缩协议
				boolean clientCompress = Capabilities.CLIENT_COMPRESS==(Capabilities.CLIENT_COMPRESS & packet.serverCapabilities);
				boolean usingCompress= MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
				if(clientCompress&&usingCompress)
				{
					source.setSupportCompress(true);
				}
				//设置ResponseHandler
				if (listener != null) {
					listener.connectionAcquired(source);
				}
				break;
			//如果为ErrorPacket，则认证失败
			case ErrorPacket.FIELD_COUNT:
				ErrorPacket err = new ErrorPacket();
				err.read(data);
				String errMsg = new String(err.message);
				LOGGER.warn("can't connect to mysql server ,errmsg:"+errMsg+" "+source);
				//source.close(errMsg);
				throw new ConnectionException(err.errno, errMsg);
			//如果是EOFPacket，则为MySQL 4.1版本，是MySQL323加密
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