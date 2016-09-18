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
package io.mycat.server.sqlhandler;

import io.mycat.server.ErrorCode;
import io.mycat.server.Fields;
import io.mycat.server.FrontendPrepareHandler;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.packet.ExecutePacket;
import io.mycat.server.packet.util.BindValue;
import io.mycat.server.packet.util.ByteUtil;
import io.mycat.server.packet.util.PreparedStatement;
import io.mycat.server.response.PreparedStmtResponse;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author mycat
 */
public class ServerPrepareHandler implements FrontendPrepareHandler {

	private static final Logger LOGGER = Logger.getLogger(ServerPrepareHandler.class);
    private MySQLFrontConnection source;
    private volatile long pstmtId;
    private Map<String, PreparedStatement> pstmtForSql;
    private Map<Long, PreparedStatement> pstmtForId;

    public ServerPrepareHandler(MySQLFrontConnection source) {
        this.source = source;
        this.pstmtId = 0L;
        this.pstmtForSql = new HashMap<String, PreparedStatement>();
        this.pstmtForId = new HashMap<Long, PreparedStatement>();
    }

    @Override
    public void prepare(String sql) {
    	LOGGER.debug("use server prepare, sql: " + sql);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForSql.get(sql)) == null) {
        	// 解析获取字段个数和参数个数
        	int columnCount = getColumnCount(sql);
        	int paramCount = getParamCount(sql);
            pstmt = new PreparedStatement(++pstmtId, sql, columnCount, paramCount);
            pstmtForSql.put(pstmt.getStatement(), pstmt);
            pstmtForId.put(pstmt.getId(), pstmt);
        }
        PreparedStmtResponse.response(pstmt, source);
    }

    @Override
    public void execute(byte[] data) {
        long pstmtId = ByteUtil.readUB4(data, 5);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForId.get(pstmtId)) == null) {
            source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pstmtId when executing.");
        } else {
            ExecutePacket packet = new ExecutePacket(pstmt);
            try {
                packet.read(data, source.getCharset());
            } catch (UnsupportedEncodingException e) {
                source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
                return;
            }
            BindValue[] bindValues = packet.values;
            // 还原sql中的动态参数为实际参数值
            String sql = prepareStmtBindValue(pstmt, bindValues);
            // 执行sql
            source.getSession2().setPrepared(true);
            LOGGER.debug("execute prepare sql: " + sql);
            source.query(sql);
        }
    }

    @Override
    public void close(byte[] data) {
    	long pstmtId = ByteUtil.readUB4(data, 5); // 获取prepare stmt id
    	LOGGER.debug("close prepare stmt, stmtId = " + pstmtId);
    	PreparedStatement pstmt = pstmtForId.remove(pstmtId);
    	if(pstmt != null) {
    		pstmtForSql.remove(pstmt.getStatement());
    	}
    }
    
    @Override
    public void clear() {
    	this.pstmtForId.clear();
    	this.pstmtForSql.clear();
    }
    
    // TODO 获取预处理语句中column的个数
    private int getColumnCount(String sql) {
    	int columnCount = 0;
    	// TODO ...
    	return columnCount;
    }
    
    // 获取预处理sql中预处理参数个数
    private int getParamCount(String sql) {
    	char[] cArr = sql.toCharArray();
    	int count = 0;
    	for(int i = 0; i < cArr.length; i++) {
    		if(cArr[i] == '?') {
    			count++;
    		}
    	}
    	return count;
    }
    
    /**
     * 组装sql语句,替换动态参数为实际参数值
     * @param pstmt
     * @param bindValues
     * @return
     */
    private String prepareStmtBindValue(PreparedStatement pstmt, BindValue[] bindValues) {
    	String sql = pstmt.getStatement();
    	int paramNumber = pstmt.getParametersNumber();
    	int[] paramTypes = pstmt.getParametersType();
    	for(int i = 0; i < paramNumber; i++) {
    		int paramType = paramTypes[i];
    		BindValue bindValue = bindValues[i];
    		if(bindValue.isNull) {
    			sql = sql.replaceFirst("\\?", "NULL");
    			continue;
    		}
    		switch(paramType) {
    		case io.mycat.server.Fields.FIELD_TYPE_TINY:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.byteBinding));
    			break;
    		case Fields.FIELD_TYPE_SHORT:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.shortBinding));
    			break;
    		case Fields.FIELD_TYPE_LONG:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.intBinding));
    			break;
    		case Fields.FIELD_TYPE_LONGLONG:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.longBinding));
    			break;
    		case Fields.FIELD_TYPE_FLOAT:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.floatBinding));
    			break;
    		case Fields.FIELD_TYPE_DOUBLE:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.doubleBinding));
    			break;
    		case Fields.FIELD_TYPE_VAR_STRING:
            case Fields.FIELD_TYPE_STRING:
            case Fields.FIELD_TYPE_VARCHAR:
            case Fields.FIELD_TYPE_BLOB:
            	sql = sql.replaceFirst("\\?", "'" + bindValue.value + "'");
            	break;
            case Fields.FIELD_TYPE_TIME:
            case Fields.FIELD_TYPE_DATE:
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
            	sql = sql.replaceFirst("\\?", "'" + bindValue.value + "'");
            	break;
            default:
            	sql = sql.replaceFirst("\\?", bindValue.value.toString());
            	break;
    		}
    	}
    	return sql;
    } 

}