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
package org.opencloudb.mpp;

import java.util.Map;
import java.util.Set;

import org.opencloudb.config.model.TableConfig;

/**
 * Table路由相关的的信息 包括分片相关字段的查询条件 关联的分片规则等
 * 
 * @author wuzhih
 * 
 */
public class TableRouteInfo {
	/**
	 * key is column name, value is set of vars. for example sql: where cola =
	 * '555' or cola in ('777','888') will be key->cala
	 * ,value->{'555','777','888'}
	 */
	public Map<String, Set<ColumnRoutePair>> columnRoutemap;
	public TableConfig matchedTable;
	// sharding rule of this table matched?
	public boolean ruleMatched;

}