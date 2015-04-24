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
package org.opencloudb.mysql;

import java.io.UnsupportedEncodingException;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;

/**
 * @author mycat
 */
public class PacketUtil {
    private static final String CODE_PAGE_1252 = "Cp1252";

    public static final ResultSetHeaderPacket getHeader(int fieldCount) {
        ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
        packet.packetId = 1;
        packet.fieldCount = fieldCount;
        return packet;
    }

    public static byte[] encode(String src, String charset) {
        if (src == null) {
            return null;
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public static final FieldPacket getField(String name, String orgName, int type) {
        FieldPacket packet = new FieldPacket();
        packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
        packet.name = encode(name, CODE_PAGE_1252);
        packet.orgName = encode(orgName, CODE_PAGE_1252);
        packet.type = (byte) type;
        return packet;
    }

    public static final FieldPacket getField(String name, int type) {
        FieldPacket packet = new FieldPacket();
        packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
        packet.name = encode(name, CODE_PAGE_1252);
        packet.type = (byte) type;
        return packet;
    }

    public static final ErrorPacket getShutdown() {
        ErrorPacket error = new ErrorPacket();
        error.packetId = 1;
        error.errno = ErrorCode.ER_SERVER_SHUTDOWN;
        error.message = "The server has been shutdown".getBytes();
        return error;
    }

    public static final FieldPacket getField(BinaryPacket src, String fieldName) {
        FieldPacket field = new FieldPacket();
        field.read(src);
        field.name = encode(fieldName, CODE_PAGE_1252);
        field.packetLength = field.calcPacketSize();
        return field;
    }

}