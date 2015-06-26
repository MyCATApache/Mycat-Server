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
import io.mycat.server.packet.util.BufferUtil;
import io.mycat.server.packet.util.StreamUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public class BinaryPacket extends MySQLPacket {
	public static final byte OK = 1;
	public static final byte ERROR = 2;
	public static final byte HEADER = 3;
	public static final byte FIELD = 4;
	public static final byte FIELD_EOF = 5;
	public static final byte ROW = 6;
	public static final byte PACKET_EOF = 7;

	public byte[] data;

	public void read(InputStream in) throws IOException {
		packetLength = StreamUtil.readUB3(in);
		packetId = StreamUtil.read(in);
		byte[] ab = new byte[packetLength];
		StreamUtil.read(in, ab, 0, ab.length);
		data = ab;
	}

	@Override
	public void write(BufferArray bufferArray) {
		int size = calcPacketSize();
		ByteBuffer buffer = bufferArray.checkWriteBuffer(packetHeaderSize
				+ size);
		BufferUtil.writeUB3(buffer, size);
		buffer.put(packetId);
		bufferArray.write(data);

	}

	@Override
	public int calcPacketSize() {
		return data == null ? 0 : data.length;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Binary Packet";
	}

}