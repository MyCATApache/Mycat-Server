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

import java.nio.ByteBuffer;

import io.mycat.backend.mysql.BufferUtil;
import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.net.FrontendConnection;

/**
 * <pre>
 * From server to client, in response to prepared statement initialization packet. 
 * It is made up of: 
 *   1.a PREPARE_OK packet
 *   2.if "number of parameters" > 0 
 *       (field packets) as in a Result Set Header Packet 
 *       (EOF packet)
 *   3.if "number of columns" > 0 
 *       (field packets) as in a Result Set Header Packet 
 *       (EOF packet)
 *   
 * -----------------------------------------------------------------------------------------
 * 
 *  Bytes              Name
 *  -----              ----
 *  1                  0 - marker for OK packet
 *  4                  statement_handler_id
 *  2                  number of columns in result set
 *  2                  number of parameters in query
 *  1                  filler (always 0)
 *  2                  warning count
 *  
 *  @see http://dev.mysql.com/doc/internals/en/prepared-statement-initialization-packet.html
 * </pre>
 * 
 * @author mycat
 */
public class PreparedOkPacket extends MySQLPacket {
    public static int PACKET_SIZE = 12;
    public byte flag;
    public long statementId;
    public int columnsNumber;
    public int parametersNumber;
    public byte filler;
    public int warningCount;

    public PreparedOkPacket() {
        this.flag = 0;
        this.filler = 0;
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        flag = mm.read();
        statementId = mm.readUB4();
        columnsNumber = mm.readUB2();
        parametersNumber = mm.readUB2();
        warningCount = mm.read();

    }
    @Override
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c,boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size,writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(flag);
        BufferUtil.writeUB4(buffer, statementId);
        BufferUtil.writeUB2(buffer, columnsNumber);
        BufferUtil.writeUB2(buffer, parametersNumber);
        buffer.put(filler);
        BufferUtil.writeUB2(buffer, warningCount);
        return buffer;
    }

    @Override
    public int calcPacketSize() {
        return PACKET_SIZE;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL PreparedOk Packet";
    }

}