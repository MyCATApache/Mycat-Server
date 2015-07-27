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
package io.mycat.server.packet;

import io.mycat.net.BufferArray;
import io.mycat.server.Capabilities;
import io.mycat.server.packet.util.BufferUtil;
import io.mycat.server.packet.util.StreamUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * From client to server during initial handshake.
 * 
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Client_Authentication_Packet
 * </pre>
 * 
 * @author mycat
 */
public class AuthPacket extends MySQLPacket {
	private static final byte[] FILLER = new byte[23];

	public long clientFlags;
	public long maxPacketSize;
	public int charsetIndex;
	public byte[] extra;// from FILLER(23)
	public String user;
	public byte[] password;
	public String database;

	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		clientFlags = mm.readUB4();
		maxPacketSize = mm.readUB4();
		charsetIndex = (mm.read() & 0xff);
		// read extra
		int current = mm.position();
		int len = (int) mm.readLength();
		if (len > 0 && len < FILLER.length) {
			byte[] ab = new byte[len];
			System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
			this.extra = ab;
		}
		mm.position(current + FILLER.length);
		user = mm.readStringWithNull();
		password = mm.readBytesWithLength();
		if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0)
				&& mm.hasRemaining()) {
			database = mm.readStringWithNull();
		}
	}

	public void write(OutputStream out) throws IOException {
		StreamUtil.writeUB3(out, calcPacketSize());
		StreamUtil.write(out, packetId);
		StreamUtil.writeUB4(out, clientFlags);
		StreamUtil.writeUB4(out, maxPacketSize);
		StreamUtil.write(out, (byte) charsetIndex);
		out.write(FILLER);
		if (user == null) {
			StreamUtil.write(out, (byte) 0);
		} else {
			StreamUtil.writeWithNull(out, user.getBytes());
		}
		if (password == null) {
			StreamUtil.write(out, (byte) 0);
		} else {
			StreamUtil.writeWithLength(out, password);
		}
		if (database == null) {
			StreamUtil.write(out, (byte) 0);
		} else {
			StreamUtil.writeWithNull(out, database.getBytes());
		}
	}

	public void write(BufferArray bufferArray) {
		int size = calcPacketSize();
		ByteBuffer buffer = bufferArray.checkWriteBuffer(packetHeaderSize
				+ size);
		BufferUtil.writeUB3(buffer, calcPacketSize());
		buffer.put(packetId);
		BufferUtil.writeUB4(buffer, clientFlags);
		BufferUtil.writeUB4(buffer, maxPacketSize);
		buffer.put((byte) charsetIndex);
		buffer = bufferArray.write(FILLER);
		if (user == null) {
			buffer = bufferArray.checkWriteBuffer(1);
			buffer.put((byte) 0);
		} else {
			byte[] userData = user.getBytes();
			buffer = bufferArray.checkWriteBuffer(userData.length + 1);
			BufferUtil.writeWithNull(buffer, userData);
		}
		if (password == null) {
			buffer = bufferArray.checkWriteBuffer(1);
			buffer.put((byte) 0);
		} else {
			buffer = bufferArray.checkWriteBuffer(BufferUtil
					.getLength(password));
			BufferUtil.writeWithLength(buffer, password);
		}
		if (database == null) {
			buffer = bufferArray.checkWriteBuffer(1);
			buffer.put((byte) 0);
		} else {
			byte[] databaseData = database.getBytes();
			buffer = bufferArray.checkWriteBuffer(databaseData.length + 1);
			BufferUtil.writeWithNull(buffer, databaseData);
		}

	}

	@Override
	public int calcPacketSize() {
		int size = 32;// 4+4+1+23;
		size += (user == null) ? 1 : user.length() + 1;
		size += (password == null) ? 1 : BufferUtil.getLength(password);
		size += (database == null) ? 1 : database.length() + 1;
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Authentication Packet";
	}

}