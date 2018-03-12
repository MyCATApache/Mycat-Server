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
package io.mycat.net.mysql;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import io.mycat.backend.mysql.BufferUtil;
import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.net.FrontendConnection;

/**
 * From server to client in response to command, if error.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errno
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 * 
 * @author mycat
 */
public class ErrorPacket extends MySQLPacket {
	public static final byte FIELD_COUNT = (byte) 0xff;
	private static final byte SQLSTATE_MARKER = (byte) '#';
	private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

	public byte fieldCount = FIELD_COUNT;
	public int errno;
	public byte mark = SQLSTATE_MARKER;
	public byte[] sqlState = DEFAULT_SQLSTATE;
	public byte[] message;

	public void read(BinaryPacket bin) {
		packetLength = bin.packetLength;
		packetId = bin.packetId;
		MySQLMessage mm = new MySQLMessage(bin.data);
		fieldCount = mm.read();
		errno = mm.readUB2();
		if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
			mm.read();
			sqlState = mm.readBytes(5);
		}
		message = mm.readBytes();
	}

	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		fieldCount = mm.read();
		errno = mm.readUB2();
		if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
			mm.read();
			sqlState = mm.readBytes(5);
		}
		message = mm.readBytes();
	}

	public byte[] writeToBytes(FrontendConnection c) {
		ByteBuffer buffer = c.allocate();
		buffer = write(buffer, c, false);
		buffer.flip();
		byte[] data = new byte[buffer.limit()];
		buffer.get(data);
		c.recycle(buffer);
		return data;
	}
	public byte[] writeToBytes() {
		ByteBuffer buffer = ByteBuffer.allocate(calcPacketSize()+4);
		int size = calcPacketSize();
		BufferUtil.writeUB3(buffer, size);
		buffer.put(packetId);
		buffer.put(fieldCount);
		BufferUtil.writeUB2(buffer, errno);
		buffer.put(mark);
		buffer.put(sqlState);
		if (message != null) {
			buffer.put(message);
		}
		buffer.flip();
		byte[] data = new byte[buffer.limit()];
		buffer.get(data);

		return data;
	}
	@Override
	public ByteBuffer write(ByteBuffer buffer, FrontendConnection c,
			boolean writeSocketIfFull) {
		int size = calcPacketSize();
		buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size,
				writeSocketIfFull);
		BufferUtil.writeUB3(buffer, size);
		buffer.put(packetId);
		buffer.put(fieldCount);
		BufferUtil.writeUB2(buffer, errno);
		buffer.put(mark);
		buffer.put(sqlState);
		if (message != null) {
			buffer = c.writeToBuffer(message, buffer);
		}
		return buffer;
	}



	public void write(FrontendConnection c) {
		ByteBuffer buffer = c.allocate();
		buffer = this.write(buffer, c, true);
		c.write(buffer);
	}

	@Override
	public int calcPacketSize() {
		int size = 9;// 1 + 2 + 1 + 5
		if (message != null) {
			size += message.length;
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Error Packet";
	}

}