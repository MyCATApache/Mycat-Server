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
package io.mycat.backend;

/**
 * connection metadata info
 * 
 * @author wuzhih
 * 
 */
public class ConnectionMeta {
	private final String schema;
	private final String charset;
	private final int txIsolation;
	private final boolean autocommit;

	public ConnectionMeta(String schema, String charset, int txIsolation,
			boolean autocommit) {
		super();
		this.schema = schema;
		this.charset = charset;
		this.txIsolation = txIsolation;
		this.autocommit = autocommit;
	}

	public String getSchema() {
		return schema;
	}

//	public String getCharset() {
//		return charset;
//	}
//
//	public int getTxIsolation() {
//		return txIsolation;
//	}
//
//	public boolean isAutocommit() {
//		return autocommit;
//	}
	
	public boolean isSameSchema(BackendConnection theCon)
	{
		return theCon.getSchema().equals(schema);
	}

	/**
	 * get metadata similarity
	 * 
	 * @param theCon
	 * @return
	 */
	public int getMetaSimilarity(BackendConnection theCon) {
		int result = 0;
		if (schema == null || schema.equals(theCon.getSchema())) {
			result++;
		}
		if (charset == null || charset.equals(theCon.getCharset())) {
			result++;
		}
		if (txIsolation == -1 || txIsolation == theCon.getTxIsolation()) {
			result++;
		}
		if (autocommit == theCon.isAutocommit()) {
			result++;
		}
		return result;
	}

	@Override
	public String toString() {
		return "ConnectionMeta [schema=" + schema + ", charset=" + charset
				+ ", txIsolation=" + txIsolation + ", autocommit=" + autocommit
				+ "]";
	}

}