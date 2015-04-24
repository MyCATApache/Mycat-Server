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
package org.opencloudb.config;

/**
 * Mycat报警关键词定义
 * 
 * @author mycat
 */
public interface Alarms {
    /** 默认报警关键词 **/
    public static final String DEFAULT           = "#!MyCat#";
    
    /** 集群无有效的节点可提供服务 **/
    public static final String CLUSTER_EMPTY     = "#!CLUSTER_EMPTY#";
    
    /** 数据节点的数据源发生切换 **/
    public static final String DATANODE_SWITCH   = "#!DN_SWITCH#";
    
    /** 隔离区非法用户访问 **/
    public static final String QUARANTINE_ATTACK = "#!QT_ATTACK#";
    
}
