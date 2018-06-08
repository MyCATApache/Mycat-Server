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
package io.mycat.manager.response;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import io.mycat.backend.heartbeat.ConsistenCollectHandler;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

/**
 * 切换数据节点的数据源
 * 
 * @author mycat
 */
public final class CheckGlobalTable {

	
	private static final int FIELD_COUNT = 9;
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
        
        fields[i] = PacketUtil.getField("FREQUENCY", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("AVG_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;  
        fields[i] = PacketUtil.getField("MAX_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("MIN_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;        
        
        fields[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        eof.packetId = ++packetId;
    }
    
    private static Map<String, String> parse(String sql) {
        Map<String, String> map = new HashMap<>();
        List<String> rtn = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings().splitToList(sql);
        for (String s : rtn) {
            if (s.contains("=")) {
                int dindex = s.indexOf("=");
                if (s.startsWith("-")) {
                    String key = s.substring(1, dindex).toLowerCase().trim();
                    String value = s.substring(dindex + 1).trim();
                    map.put(key, value);
                } else if (s.startsWith("--")) {
                    String key = s.substring(2, dindex).toLowerCase().trim();
                    String value = s.substring(dindex + 1).trim();
                    map.put(key, value);
                }
            }
        }
        return map;
    }
    
    public static void execute(ManagerConnection c
    		,String stmt) {
    	Map<String,String> paramster = parse(stmt);
    	int retryTime = 1;
    	long intervalTime = 200 ;
    	String tableName = paramster.get("tablename");
    	String schemaName = paramster.get("schemaname");
    	String retryTimeStr = paramster.get("retrytime");
    	String intervalTimeStr = paramster.get("intervaltime");
    	if(StringUtil.isEmpty(tableName)) {
//    		c.writeErrMessage(ErrorCode.ER_BAD_TABLE_ERROR, msg);
    	}    	
    	tableName = "e_account_subject";

    	schemaName = "TESTDB";
    	ConsistenCollectHandler cHandler = new ConsistenCollectHandler(c, tableName, schemaName, retryTime, intervalTime);
    	cHandler.startDetector();
    	c.writeErrMessage(ErrorCode.ER_BAD_TABLE_ERROR, "XXX");
    }

	private static RowDataPacket getRow(int i, String user, String sql, long count, long avgTime, long maxTime,
			long minTime, long executTime, long lastTime, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(LongUtil.toBytes(i));
		row.add(StringUtil.encode(user, charset));
		row.add(LongUtil.toBytes(count));
		row.add(LongUtil.toBytes(avgTime));
		row.add(LongUtil.toBytes(maxTime));
		row.add(LongUtil.toBytes(minTime));
		row.add(LongUtil.toBytes(executTime));
		row.add(LongUtil.toBytes(lastTime));
		row.add(StringUtil.encode(sql, charset));
		return row;
	}
	
	
	
	
	
	
	
    public static void response(String stmt, ManagerConnection c) {
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

//        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
//    	for (UserStat userStat : statMap.values()) {
//        	String user = userStat.getUser();
//            List<SqlFrequency> list=userStat.getSqlHigh().getSqlFrequency( isClear );
//             if ( list != null ) {
//                int i = 1;
//     	        for (SqlFrequency sqlFrequency : list) {
//					if(sqlFrequency != null){
//                        RowDataPacket row = getRow(i, user, sqlFrequency.getSql(), sqlFrequency.getCount(),
//							sqlFrequency.getAvgTime(), sqlFrequency.getMaxTime(), sqlFrequency.getMinTime(),
//							sqlFrequency.getExecuteTime(), sqlFrequency.getLastTime(), c.getCharset());
//     	                row.packetId = ++packetId;
//     	                buffer = row.write(buffer, c,true);
//     	                i++;
//                    }
//                }
//             }
//    	}    
    	
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

}