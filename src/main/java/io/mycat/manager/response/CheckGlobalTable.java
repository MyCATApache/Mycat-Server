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

import io.mycat.MycatServer;
import io.mycat.backend.heartbeat.ConsistenCollectHandler;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

/**
 * 全局表一致性检测
 * 
 * @author mycat
 */
public final class CheckGlobalTable {

	
	private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("TABLENAME", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("RESTULT", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;
                
        eof.packetId = ++packetId;
    }
    
    private static Map<String, String> parse(String sql) {
        Map<String, String> map = new HashMap<>();
        List<String> rtn = Splitter.on(CharMatcher.whitespace()).omitEmptyStrings().splitToList(sql);
        for (String s : rtn) {
            if (s.contains("=")) {
                int dindex = s.indexOf("=");
                if (s.startsWith("--")) {
                    String key = s.substring(2, dindex).toLowerCase().trim();
                    String value = s.substring(dindex + 1).trim();
                    map.put(key, value);
                } else if (s.startsWith("-")) {
                    String key = s.substring(1, dindex).toLowerCase().trim();
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
    	//"show @@CKECK_GLOBAL -SCHEMA=TESTDB -TABLE=E_ACCOUNT_SUBJECT -retrytime=2"
//		+ " -intervaltime=20"
    	String tableName = paramster.get("table");
    	String schemaName = paramster.get("schema");
    	String retryTimeStr = paramster.get("retry");
    	String intervalTimeStr = paramster.get("interval");
		MycatConfig config = MycatServer.getInstance().getConfig();
		TableConfig table;
		SchemaConfig schemaConfig = null;
    	if(StringUtil.isEmpty(schemaName)) {
    		c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "schemaName is null, please add paramster  -schema=schemaname ");
    		return;
    	} else {
    		schemaConfig = config.getSchemas().get(schemaName);
    		if(schemaConfig == null){
        		c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR,
        				"schemaName is null, please add paramster  -schema=schemaname ");

    		}
    	} 
    	if(StringUtil.isEmpty(tableName)) {
    		c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "tableName is null, please add paramster  -table=tablename ");
    		return;
    	} else {
    		 table = schemaConfig.getTables().get(tableName.toUpperCase());

    	}
    	if(StringUtil.isEmpty(retryTimeStr)) {
    		c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "retryTime is null, please add paramster  -retry= ");
    		return;
    	}else {
    		retryTime =  Integer.valueOf(retryTimeStr);
    	}
    	
    	if(StringUtil.isEmpty(intervalTimeStr)) {
    		c.writeErrMessage(ErrorCode.ER_BAD_TABLE_ERROR, "intervalTime is null, please add paramster  -interval= ");
    		return;
    	} else {
    		intervalTime = Long.valueOf(intervalTimeStr);
    	} 
    
//    	tableName = "e_account_subject";

//    	schemaName = "TESTDB";
    	


		List<String> dataNodeList = table.getDataNodes();
		
    	ConsistenCollectHandler cHandler = new ConsistenCollectHandler( c, tableName, schemaName, dataNodeList.size(), retryTime, intervalTime);
    	cHandler.startDetector();
    	//c.writeErrMessage(ErrorCode.ER_BAD_TABLE_ERROR, "XXX");
    }

	private static RowDataPacket getRow(int i, String tableName, String result,String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(LongUtil.toBytes(i));
		row.add(StringUtil.encode(tableName, charset));
		row.add(StringUtil.encode(result, charset));
		return row;
	}	
    public static void response(ManagerConnection c,String tableName, String result) {
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
        RowDataPacket row = getRow(1, tableName, result, c.getCharset());
        row.packetId = ++packetId;
        buffer = row.write(buffer, c, true);
    
    	
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }
    public static void main(String[] args) {
    	// show @@check_global --schema=TESTDB -table=e_account_subject -retry=40 -interval=20;
    	Map<String, String> params = CheckGlobalTable.parse("show @@CKECK_GLOBAL -SCHEMA=TESTDB -TABLE=E_ACCOUNT_SUBJECT -retrytime=2"
    			+ " -intervaltime=20");
    	System.out.println(params);
	}
}