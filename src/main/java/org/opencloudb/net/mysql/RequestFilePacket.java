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

import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.MySQLMessage;
import org.opencloudb.mysql.StreamUtil;
import org.opencloudb.net.BackendAIOConnection;
import org.opencloudb.net.FrontendConnection;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class RequestFilePacket extends MySQLPacket {

    public byte command= (byte) 251;
    public byte[] fileName;

    public void write(OutputStream out) throws IOException {
        StreamUtil.writeUB3(out, calcPacketSize());
        StreamUtil.write(out, packetId);
        StreamUtil.write(out, command);

    }
    @Override
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c,boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size,writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put((byte) 0);
        buffer.put(command);
        c.write(buffer);
     //   buffer.put(fileName);
    //    BufferUtil.writeInt(buffer,fileName.length);
        return buffer;
    }

    @Override
    public int calcPacketSize() {
        return 1;
     //   return 5+BufferUtil.getLength(fileName) +BufferUtil.getLength(fileName.length) ;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Command Packet";
    }

	
}