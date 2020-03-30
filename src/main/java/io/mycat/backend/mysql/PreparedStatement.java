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
package io.mycat.backend.mysql;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.server.response.PreparedStmtResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat, CrazyPig
 */
public class PreparedStatement {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatement.class);
    private long id;
    private String statement;
    private String[] columnNames;
    private int parametersNumber;
    private int[] parametersType;
    /**
     * 存放COM_STMT_SEND_LONG_DATA命令发送过来的字节数据
     * <pre>
     * key : param_id
     * value : byte data
     * </pre>
     */
    private Map<Long, ByteArrayOutputStream> longDataMap;
    //获取预处理语句中column的个数
    private static String[] getColumns(String sql) {
        String[] columnNames;
        try {
            SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, JdbcUtils.MYSQL);
            SQLStatement statement = sqlStatementParser.parseStatement();
            if (statement instanceof SQLSelectStatement) {
                SQLSelect select = ((SQLSelectStatement) statement).getSelect();
                com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock query = (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock) select.getQuery();
                int size = query.getSelectList().size();
                if (size == 1){
                    if("*".equalsIgnoreCase(   query.getSelectList().get(0).toString())){
                        throw new Exception("unsupport * in select items:"+sql);
                    }
                } {
                    columnNames = new String[size];
                    for (int i = 0; i < size; i++) {
                        columnNames[i] = query.getSelectList().get(i).toString();
                    }
                    return columnNames;
                }

            }
        }catch (Exception e){
            LOGGER.error("can not get column count",e);
        }
        return new String[]{};
    }
    public PreparedStatement(long id, String statement, int parametersNumber) {
        this.id = id;
        this.statement = statement;
        this.columnNames = getColumns(statement);
        this.parametersNumber = parametersNumber;
        this.parametersType = new int[parametersNumber];
        this.longDataMap = new HashMap<Long, ByteArrayOutputStream>();
    }

    public long getId() {
        return id;
    }

    public String getStatement() {
        return statement;
    }

    public int getColumnsNumber() {
        return this.columnNames.length;
    }

    public int getParametersNumber() {
        return parametersNumber;
    }

    public int[] getParametersType() {
        return parametersType;
    }
    
    public boolean hasLongData(long paramId) {
    	return longDataMap.containsKey(paramId);
    }

    public ByteArrayOutputStream getLongData(long paramId) {
    	return longDataMap.get(paramId);
    }
    
    /**
     * COM_STMT_RESET命令将调用该方法进行数据重置
     */
    public void resetLongData() {
    	for(Long paramId : longDataMap.keySet()) {
    		longDataMap.get(paramId).reset();
    	}
    }
    
    /**
     * 追加数据到指定的预处理参数
     * @param paramId
     * @param data
     * @throws IOException
     */
    public void appendLongData(long paramId, byte[] data) throws IOException {
    	if(getLongData(paramId) == null) {
    		ByteArrayOutputStream out = new ByteArrayOutputStream();
        	out.write(data);
    		longDataMap.put(paramId, out);
    	} else {
    		longDataMap.get(paramId).write(data);
    	}
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}