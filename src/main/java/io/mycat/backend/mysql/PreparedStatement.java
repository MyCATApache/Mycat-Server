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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat, CrazyPig
 */
public class PreparedStatement {

    private long id;
    private String statement;
    private int columnsNumber;
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

    public PreparedStatement(long id, String statement, int columnsNumber, int parametersNumber) {
        this.id = id;
        this.statement = statement;
        this.columnsNumber = columnsNumber;
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
        return columnsNumber;
    }

    public int getParametersNumber() {
        return parametersNumber;
    }

    public int[] getParametersType() {
        return parametersType;
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
}