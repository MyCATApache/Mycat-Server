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

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.mysql.PreparedStatement;
import io.mycat.net.FrontendConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.PreparedOkPacket;

/**
 * @author mycat
 */
public class PreparedStmtResponse {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStmtResponse.class);
    public static void response(PreparedStatement pstmt, FrontendConnection c) {
        if (pstmt.getParams() != null || pstmt.getFields() != null) {
            LOGGER.info("prepared metadata from backend mysql data and is complete");
            responseCompleteMetaData(pstmt, c);
        } else {
            LOGGER.info("prepared metadata comes from self-construction and is incomplete");
            responseIncompleteMetaData(pstmt, c);
        }
    }

    private static void responseCompleteMetaData(PreparedStatement pstmt, FrontendConnection c) {
        byte packetId = 0;

        // write preparedOk packet
        PreparedOkPacket preparedOk = new PreparedOkPacket();
        preparedOk.packetId = ++packetId;
        preparedOk.statementId = pstmt.getId();
        preparedOk.columnsNumber = pstmt.getColumnsNumber();
        preparedOk.parametersNumber = pstmt.getParametersNumber();
        ByteBuffer buffer = preparedOk.write(c.allocate(), c, true);

        // write parameter field packet
        int parametersNumber = pstmt.getParametersNumber();
        if (parametersNumber > 0) {
            for (FieldPacket param : pstmt.getParams()) {
                param.packetId = ++packetId;
                buffer = param.write(buffer, c, true);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            buffer = eof.write(buffer, c, true);
        }

        // write column field packet
        int columnsNumber = pstmt.getColumnsNumber();
        if (columnsNumber > 0) {
            for (FieldPacket field : pstmt.getFields()) {
                field.packetId = ++packetId;
                buffer = field.write(buffer, c, true);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            buffer = eof.write(buffer, c, true);
        }

        // send buffer
        c.write(buffer);
    }

    /**
     * 这种返回结果，元数据不全。用c驱动读取元数据信息会报错，比如下面的用法：
     *   prepare_meta_result = mysql_stmt_result_metadata(stmt); 
     *   column_count= mysql_num_fields(prepare_meta_result); 
     * @param pstmt
     * @param c
     */
    private static void responseIncompleteMetaData(PreparedStatement pstmt, FrontendConnection c) {
        byte packetId = 0;

        // write preparedOk packet
        PreparedOkPacket preparedOk = new PreparedOkPacket();
        preparedOk.packetId = ++packetId;
        preparedOk.statementId = pstmt.getId();
        preparedOk.columnsNumber = pstmt.getColumnsNumber();
        preparedOk.parametersNumber = pstmt.getParametersNumber();
        ByteBuffer buffer = preparedOk.write(c.allocate(), c, true);

        // write parameter field packet
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

        // write column field packet
        int columnsNumber = preparedOk.columnsNumber;
        if (columnsNumber > 0) {
            String[] columnNames = pstmt.getColumnNames();
            for (int i = 0; i < columnsNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.name= columnNames[i].getBytes();
                field.packetId = ++packetId;
                buffer = field.write(buffer, c,true);
            }
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId;
            buffer = eof.write(buffer, c,true);
        }

        // send buffer
        c.write(buffer);
    }

}