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
package io.mycat.server.response;

import io.mycat.backend.mysql.PreparedStatement;
import io.mycat.net.FrontendConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.PreparedOkPacket;

import java.nio.ByteBuffer;

/**
 * 预处理语句响应
 * @author mycat
 */
public class PreparedStmtResponse {

    /**
     * 响应
     * @param pstmt
     * @param c
     */
    public static void response(PreparedStatement pstmt, FrontendConnection c) {
        byte packetId = 0;

        // 写预处理的OK包
        PreparedOkPacket preparedOk = new PreparedOkPacket();
        preparedOk.packetId = ++packetId;
        preparedOk.statementId = pstmt.getId();
        preparedOk.columnsNumber = pstmt.getColumnsNumber();
        preparedOk.parametersNumber = pstmt.getParametersNumber();
        ByteBuffer buffer = preparedOk.write(c.allocate(), c,true);

        // 写参数字段包
        int parametersNumber = preparedOk.parametersNumber;
        if (parametersNumber > 0) {
            for (int i = 0; i < parametersNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.packetId = ++packetId;
                buffer = field.write(buffer, c,true);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            buffer = eof.write(buffer, c,true);
        }

        // 写列字段包
        int columnsNumber = preparedOk.columnsNumber;
        if (columnsNumber > 0) {
            for (int i = 0; i < columnsNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.packetId = ++packetId;
                buffer = field.write(buffer, c,true);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            buffer = eof.write(buffer, c,true);
        }

        // 发送缓冲区
        c.write(buffer);
    }

}