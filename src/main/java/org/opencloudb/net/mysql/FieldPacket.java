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
package org.opencloudb.net.mysql;

import java.nio.ByteBuffer;

import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.MySQLMessage;
import org.opencloudb.net.FrontendConnection;

/**
 * From Server To Client, part of Result Set Packets. One for each column in the
 * result set. Thus, if the value of field_columns in the Result Set Header
 * Packet is 3, then the Field Packet occurs 3 times.
 * 
 * <pre>
 * Bytes                      Name
 * -----                      ----
 * n (Length Coded String)    catalog
 * n (Length Coded String)    db
 * n (Length Coded String)    table
 * n (Length Coded String)    org_table
 * n (Length Coded String)    name
 * n (Length Coded String)    org_name
 * 1                          (filler)
 * 2                          charsetNumber
 * 4                          length
 * 1                          type
 * 2                          flags
 * 1                          decimals
 * 2                          (filler), always 0x00
 * n (Length Coded Binary)    default
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Field_Packet
 * </pre>
 * 
 * @author mycat
 */
public class FieldPacket extends MySQLPacket {
	private static final byte[] DEFAULT_CATALOG = "def".getBytes();
	private static final byte[] FILLER = new byte[2];

	public byte[] catalog = DEFAULT_CATALOG;
	public byte[] db;
	public byte[] table;
	public byte[] orgTable;
	public byte[] name;
	public byte[] orgName;
	public int charsetIndex;
	public long length;
	public int type;
	public int flags;
	public byte decimals;
	public byte[] definition;

	/**
	 * 把字节数组转变成FieldPacket
	 */
	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		this.packetLength = mm.readUB3();
		this.packetId = mm.read();
		readBody(mm);
	}

	/**
	 * 把BinaryPacket转变成FieldPacket
	 */
	public void read(BinaryPacket bin) {
		this.packetLength = bin.packetLength;
		this.packetId = bin.packetId;
		readBody(new MySQLMessage(bin.data));
	}

	@Override
	public ByteBuffer write(ByteBuffer buffer, FrontendConnection c,
			boolean writeSocketIfFull) {
		int size = calcPacketSize();
		buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size,
				writeSocketIfFull);
		BufferUtil.writeUB3(buffer, size);
		buffer.put(packetId);
		writeBody(buffer);
		return buffer;
	}

	@Override
	public int calcPacketSize() {
		int size = (catalog == null ? 1 : BufferUtil.getLength(catalog));
		size += (db == null ? 1 : BufferUtil.getLength(db));
		size += (table == null ? 1 : BufferUtil.getLength(table));
		size += (orgTable == null ? 1 : BufferUtil.getLength(orgTable));
		size += (name == null ? 1 : BufferUtil.getLength(name));
		size += (orgName == null ? 1 : BufferUtil.getLength(orgName));
		size += 13;// 1+2+4+1+2+1+2
		if (definition != null) {
			size += BufferUtil.getLength(definition);
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Field Packet";
	}

	private void readBody(MySQLMessage mm) {
		this.catalog = mm.readBytesWithLength();
		this.db = mm.readBytesWithLength();
		this.table = mm.readBytesWithLength();
		this.orgTable = mm.readBytesWithLength();
		this.name = mm.readBytesWithLength();
		this.orgName = mm.readBytesWithLength();
		mm.move(1);
		this.charsetIndex = mm.readUB2();
		this.length = mm.readUB4();
		this.type = mm.read() & 0xff;
		this.flags = mm.readUB2();
		this.decimals = mm.read();
		mm.move(FILLER.length);
		if (mm.hasRemaining()) {
			this.definition = mm.readBytesWithLength();
		}
	}

	private void writeBody(ByteBuffer buffer) {
		byte nullVal = 0;
		BufferUtil.writeWithLength(buffer, catalog, nullVal);
		BufferUtil.writeWithLength(buffer, db, nullVal);
		BufferUtil.writeWithLength(buffer, table, nullVal);
		BufferUtil.writeWithLength(buffer, orgTable, nullVal);
		BufferUtil.writeWithLength(buffer, name, nullVal);
		BufferUtil.writeWithLength(buffer, orgName, nullVal);
		buffer.put((byte) 0x0C);
		BufferUtil.writeUB2(buffer, charsetIndex);
		BufferUtil.writeUB4(buffer, length);
		buffer.put((byte) (type & 0xff));
		BufferUtil.writeUB2(buffer, flags);
		buffer.put(decimals);
        buffer.put((byte)0x00);
        buffer.put((byte)0x00);
		//buffer.position(buffer.position() + FILLER.length);
		if (definition != null) {
			BufferUtil.writeWithLength(buffer, definition);
		}
	}

}