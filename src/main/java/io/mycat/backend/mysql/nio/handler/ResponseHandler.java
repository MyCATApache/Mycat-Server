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
package io.mycat.backend.mysql.nio.handler;

import java.util.List;

import io.mycat.backend.BackendConnection;

/**
 * @author mycat
 * @author mycat
 */
public interface ResponseHandler {

	/**
	 * 无法获取连接
	 * 
	 * @param e
	 * @param conn
	 */
	public void connectionError(Throwable e, BackendConnection conn);

	/**
	 * 已获得有效连接的响应处理
	 */
	void connectionAcquired(BackendConnection conn);

	/**
	 * 收到错误数据包的响应处理
	 */
	void errorResponse(byte[] err, BackendConnection conn);

	/**
	 * 收到OK数据包的响应处理
	 */
	void okResponse(byte[] ok, BackendConnection conn);

	/**
	 * 收到字段数据包结束的响应处理
	 */
	void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
			BackendConnection conn);

	/**
	 * 收到行数据包的响应处理
	 */
	void rowResponse(byte[] row, BackendConnection conn);

	/**
	 * 收到行数据包结束的响应处理
	 */
	void rowEofResponse(byte[] eof, BackendConnection conn);

	/**
	 * 写队列为空，可以写数据了
	 * 
	 */
	void writeQueueAvailable();

	/**
	 * on connetion close event
	 */
	void connectionClose(BackendConnection conn, String reason);

	
}