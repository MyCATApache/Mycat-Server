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
package io.mycat.net.handler;

import java.util.Set;

/**
 * 权限提供者
 * 
 * @author mycat
 */
public interface FrontendPrivileges {

    /**
     * 检查schema是否存在
     */
    boolean schemaExists(String schema);

    /**
     * 检查用户是否存在，并且可以使用host实行隔离策略。
     */
    boolean userExists(String user, String host);

    /**
     * 提供用户的服务器端密码
     */
    String getPassword(String user);

    /**
     * 提供有效的用户schema集合
     */
    Set<String> getUserSchemas(String user);
    
    /**
     * 检查用户是否为只读权限
     * @param user
     * @return
     */
    Boolean isReadOnly(String user);
    
    /**
     * 获取设定的系统最大连接数的降级阀值
     * @param user
     * @return
     */
    int getBenchmark(String user);
    
    
    /**
     * 检查防火墙策略
     * （白名单策略）
     * @param user
     * @param host
     * @return
     */
    boolean checkFirewallWhiteHostPolicy(String user, String host);
    
    /**
     * 检查防火墙策略
     * (SQL黑名单及注入策略)
     * @param sql
     * @return
     */
    boolean checkFirewallSQLPolicy(String user, String sql);
    
    
    /**
     * 检查 SQL 语句的 DML 权限
     * @return
     */
    boolean checkDmlPrivilege(String user, String schema, String sql);

    /**
     * 检查针对 DataNode 的 SQL 语句的 DML 权限
     * @param user
     * @param dataNode
     * @param sql
     * @return
     */
    boolean checkDataNodeDmlPrivilege(String user, String dataNode, String sql);

}