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

import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;

/**
 * 事务隔离级别定义
 * 
 * @author mycat
 */
public interface Isolations {

    public static final int READ_UNCOMMITTED = 1;
    public static final int READ_COMMITTED = 2;
    public static final int REPEATED_READ = 3;
    public static final int SERIALIZABLE = 4;

    /**
     * 通用的RDBMS事务隔离级别的数值应该为 0，1，2，3，由于mycat历史遗留问题，目前
     * 仍然使用从1开始表示各个级别。对应的MYSQL的tx_isolation变量取值参考
     * http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html
     */
    public static final String[] IsolationLevel = {
            "READ-UNCOMMITTED", // 0 占位
            "READ-UNCOMMITTED", // 1 也叫 DIRTY READ
            "READ-COMMITTED",   // 2 缩写 RC
            "REPEATABLE-READ",  // 3 缩写 RR
            "SERIALIZABLE"      // 4 缩写 SR
    };
}