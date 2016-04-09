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
package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.util.Map;

import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.stat.UserStatAnalyzer;

import org.opencloudb.stat.UserSqlStat;
import org.opencloudb.stat.UserStat;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.StringUtil;


/**
 * 查询用户最近执行的SQL记录
 * 
 * @author mycat
 * @author zhuam
 */
public final class ShowSQL {

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;        
        
        fields[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
  
        fields[i] = PacketUtil.getField("IP", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;        
        
        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c, boolean isClear) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }

        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        byte packetId = eof.packetId;        
        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
    	for (UserStat userStat : statMap.values()) {
        	String user = userStat.getUser();
            UserSqlStat.Sql[] sqls = userStat.getSqlStat().getSqls();
            for (int i = sqls.length - 1; i >= 0; i--) {
                if (sqls[i] != null) {
                    RowDataPacket row = getRow(user, sqls[i], i, c.getCharset());
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, c,true);
                }
            }
            
            //读取SQL监控后清理
            if ( isClear ) {
            	userStat.getSqlStat().clear();
            }
        }

        
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String user, UserSqlStat.Sql sql, int idx, String charset) {
        
    	RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));          
        row.add( StringUtil.encode( user, charset) );
        row.add( LongUtil.toBytes( sql.getStartTime() ) );
        row.add( LongUtil.toBytes( sql.getExecuteTime() ) );
        row.add( StringUtil.encode( sql.getSql(), charset) );
        row.add( StringUtil.encode( sql.getIp(), charset) );
        return row;
    }

}