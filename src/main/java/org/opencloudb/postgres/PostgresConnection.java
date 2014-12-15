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
package org.opencloudb.postgres;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.AsynchronousSocketChannel;

import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.BackendAIOConnection;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;

/**
 * @author mycat
 */
public class PostgresConnection extends BackendAIOConnection {

	public PostgresConnection(AsynchronousSocketChannel channel) {
		super(channel);

	}

	@Override
	public void onConnectFailed(Throwable e) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isModifiedSQLExecuted() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFromSlaveDB() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSchema(String newSchema) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getLastTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isClosedOrQuit() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setAttachment(Object attachment) {
		// TODO Auto-generated method stub

	}

	@Override
	public void quit() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setLastTime(long currentTimeMillis) {
		// TODO Auto-generated method stub

	}

	@Override
	public void release() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean setResponseHandler(ResponseHandler commandHandler) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub

	}

	@Override
	public void query(String sql) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub

	}

	@Override
	public Object getAttachment() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCharset() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(RouteResultsetNode node, ServerConnection source,
			boolean autocommit) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void recordSql(String host, String schema, String statement) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean syncAndExcute() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isBorrowed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setBorrowed(boolean borrowed) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getTxIsolation() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isAutocommit() {
		// TODO Auto-generated method stub
		return false;
	}

}