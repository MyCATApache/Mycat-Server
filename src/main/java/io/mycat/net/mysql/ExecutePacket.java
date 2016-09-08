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

import java.io.UnsupportedEncodingException;

import io.mycat.backend.mysql.BindValue;
import io.mycat.backend.mysql.BindValueUtil;
import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.backend.mysql.PreparedStatement;

/**
 * <pre>
 *  Bytes                      Name
 *  -----                      ----
 *  1                          code
 *  4                          statement_id
 *  1                          flags
 *  4                          iteration_count 
 *  (param_count+7)/8          null_bit_map
 *  1                          new_parameter_bound_flag (if new_params_bound == 1:)
 *  n*2                        type of parameters
 *  n                          values for the parameters   
 *  --------------------------------------------------------------------------------
 *  code:                      always COM_EXECUTE
 *  
 *  statement_id:              statement identifier
 *  
 *  flags:                     reserved for future use. In MySQL 4.0, always 0.
 *                             In MySQL 5.0: 
 *                               0: CURSOR_TYPE_NO_CURSOR
 *                               1: CURSOR_TYPE_READ_ONLY
 *                               2: CURSOR_TYPE_FOR_UPDATE
 *                               4: CURSOR_TYPE_SCROLLABLE
 *  
 *  iteration_count:           reserved for future use. Currently always 1.
 *  
 *  null_bit_map:              A bitmap indicating parameters that are NULL.
 *                             Bits are counted from LSB, using as many bytes
 *                             as necessary ((param_count+7)/8)
 *                             i.e. if the first parameter (parameter 0) is NULL, then
 *                             the least significant bit in the first byte will be 1.
 *  
 *  new_parameter_bound_flag:  Contains 1 if this is the first time
 *                             that "execute" has been called, or if
 *                             the parameters have been rebound.
 *  
 *  type:                      Occurs once for each parameter; 
 *                             The highest significant bit of this 16-bit value
 *                             encodes the unsigned property. The other 15 bits
 *                             are reserved for the type (only 8 currently used).
 *                             This block is sent when parameters have been rebound
 *                             or when a prepared statement is executed for the 
 *                             first time.
 * 
 *  values:                    for all non-NULL values, each parameters appends its value
 *                             as described in Row Data Packet: Binary (column values)
 * @see http://dev.mysql.com/doc/internals/en/execute-packet.html
 * </pre>
 * 
 * @author mycat
 */
public class ExecutePacket extends MySQLPacket {

    public byte code;
    public long statementId;
    public byte flags;
    public long iterationCount;
    public byte[] nullBitMap;
    public byte newParameterBoundFlag;
    public BindValue[] values;
    protected PreparedStatement pstmt;

    public ExecutePacket(PreparedStatement pstmt) {
        this.pstmt = pstmt;
        this.values = new BindValue[pstmt.getParametersNumber()];
    }

    public void read(byte[] data, String charset) throws UnsupportedEncodingException {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        code = mm.read();
        statementId = mm.readUB4();
        flags = mm.read();
        iterationCount = mm.readUB4();

        // 读取NULL指示器数据
        int parameterCount = values.length;
        nullBitMap = new byte[(parameterCount + 7) / 8];
        for (int i = 0; i < nullBitMap.length; i++) {
            nullBitMap[i] = mm.read();
        }

        // 当newParameterBoundFlag==1时，更新参数类型。
        newParameterBoundFlag = mm.read();
        if (newParameterBoundFlag == (byte) 1) {
            for (int i = 0; i < parameterCount; i++) {
                pstmt.getParametersType()[i] = mm.readUB2();
            }
        }

        // 设置参数类型和读取参数值
        byte[] nullBitMap = this.nullBitMap;
        for (int i = 0; i < parameterCount; i++) {
            BindValue bv = new BindValue();
            bv.type = pstmt.getParametersType()[i];
            if ((nullBitMap[i / 8] & (1 << (i & 7))) != 0) {
                bv.isNull = true;
            } else {
                BindValueUtil.read(mm, bv, charset);
                if(bv.isLongData) {
                	bv.value = pstmt.getLongData(i);
                }
            }
            values[i] = bv;
        }
    }

    @Override
    public int calcPacketSize() {
        
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Execute Packet";
    }

}