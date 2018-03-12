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
package io.mycat.sqlengine.mpp;

import java.io.Serializable;

public class ColMeta implements Serializable{
	public static final int COL_TYPE_DECIMAL = 0;
	public static final int COL_TYPE_INT = 1;
	public static final int COL_TYPE_SHORT = 2;
	public static final int COL_TYPE_LONG = 3;
	public static final int COL_TYPE_FLOAT = 4;
	public static final int COL_TYPE_DOUBLE = 5;
	public static final int COL_TYPE_NULL = 6;
	public static final int COL_TYPE_TIMSTAMP = 7;
	public static final int COL_TYPE_LONGLONG = 8;
	public static final int COL_TYPE_INT24 = 9;
	public static final int COL_TYPE_DATE = 0x0a;
	public static final int COL_TYPE_DATETIME=0X0C;
	public static final int COL_TYPE_TIME = 0x0b;
	public static final int COL_TYPE_YEAR = 0x0d;
	public static final int COL_TYPE_NEWDATE = 0x0e;
	public static final int COL_TYPE_VACHAR = 0x0f;
	public static final int COL_TYPE_BIT = 0x10;
	public static final int COL_TYPE_NEWDECIMAL = 0xf6;
	public static final int COL_TYPE_ENUM = 0xf7;
	public static final int COL_TYPE_SET = 0xf8;
	public static final int COL_TYPE_TINY_BLOB = 0xf9;
	public static final int COL_TYPE_TINY_TYPE_MEDIUM_BLOB = 0xfa;
	public static final int COL_TYPE_TINY_TYPE_LONG_BLOB = 0xfb;
	public static final int COL_TYPE_BLOB = 0xfc;
	public static final int COL_TYPE_VAR_STRING = 0xfd;
	public static final int COL_TYPE_STRING = 0xfe;
	public static final int COL_TYPE_GEOMETRY = 0xff;
	public  int colIndex;
	public final int colType;
	
	public int decimals;

    public  int avgSumIndex;
    public  int avgCountIndex;

    public ColMeta(int colIndex, int colType) {
		super();
		this.colIndex = colIndex;
		this.colType = colType;
	}
    public ColMeta(int avgSumIndex,int avgCountIndex,  int colType) {
        super();
        this.avgSumIndex = avgSumIndex;
        this.avgCountIndex=avgCountIndex;
        this.colType = colType;
    }
	public int getColIndex() {
		return colIndex;
	}

	public int getColType() {
		return colType;
	}

	@Override
	public String toString() {
		return "ColMeta [colIndex=" + colIndex + ", colType=" + colType + "]";
	}

}