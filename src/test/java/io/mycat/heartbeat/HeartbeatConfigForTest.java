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
package io.mycat.heartbeat;

import org.junit.Test;

/**
 * @author mycat
 */
public class HeartbeatConfigForTest {
    @Test
    public void testNoop() {
    }
    // public static DataNodeConfig[] getOfferNodes(int offset, int length) {
    // DataNodeConfig[] nodes = new DataNodeConfig[length];
    // for (int i = 0; i < length; i++) {
    // DataNodeConfig node = new DataNodeConfig();
    // node.name = "offer" + (offset + i);
    // node.activedIndex = 0;
    // node.dataSource = getOfferDataSource(node.name);
    // nodes[i] = node;
    // }
    // return nodes;
    // }
    //
    // private static DataSourceConfig[] getOfferDataSource(String schema) {
    // DataSourceConfig ds1 = new DataSourceConfig();
    // ds1.host = "10.20.132.17";
    // ds1.port = 3306;
    // ds1.schema = schema;
    // ds1.user = "offer";
    // ds1.password = "offer";
    // ds1.statement = "update xdual set x=now()";
    //
    // DataSourceConfig ds2 = new DataSourceConfig();
    // ds2.host = "10.20.153.177";
    // ds2.port = 3316;
    // ds2.schema = schema;
    // ds2.user = "offer";
    // ds2.password = "offer";
    // ds2.statement = "update xdual set x=now()";
    //
    // return new DataSourceConfig[] { ds1, ds2 };
    // }
    //
    // public static DataNodeConfig getNodeErrorConfig() {
    // // 数据源1（IP错误）
    // DataSourceConfig ds1 = new DataSourceConfig();
    // ds1.host = "100.20.132.17";
    // ds1.port = 3306;
    // ds1.schema = "offer1";
    // ds1.user = "offer";
    // ds1.password = "offer";
    // ds1.statement = "update xdual set x=now()";
    //
    // // 数据源2（端口错误）
    // DataSourceConfig ds2 = new DataSourceConfig();
    // ds2.host = "10.20.132.17";
    // ds2.port = 3316;
    // ds2.schema = "offer1";
    // ds2.user = "offer";
    // ds2.password = "offer";
    // ds2.statement = "update xdual set x=now()";
    //
    // // 数据源3（SCHEMA错误）
    // DataSourceConfig ds3 = new DataSourceConfig();
    // ds3.host = "10.20.132.17";
    // ds3.port = 3306;
    // ds3.schema = "offer1_x";
    // ds3.user = "offer";
    // ds3.password = "offer";
    // ds3.statement = "update xdual set x=now()";
    //
    // // 数据源4（用户错误）
    // DataSourceConfig ds4 = new DataSourceConfig();
    // ds4.host = "10.20.132.17";
    // ds4.port = 3306;
    // ds4.schema = "offer1";
    // ds4.user = "offer_x";
    // ds4.password = "offer";
    // ds4.statement = "update xdual set x=now()";
    //
    // // 数据源5（密码错误）
    // DataSourceConfig ds5 = new DataSourceConfig();
    // ds5.host = "10.20.132.17";
    // ds5.port = 3306;
    // ds5.schema = "offer1";
    // ds5.user = "offer";
    // ds5.password = "offer_x";
    // ds5.statement = "update xdual set x=now()";
    //
    // // 数据源6（语句错误）
    // DataSourceConfig ds6 = new DataSourceConfig();
    // ds6.host = "10.20.132.17";
    // ds6.port = 3306;
    // ds6.schema = "offer1";
    // ds6.user = "offer";
    // ds6.password = "offer";
    // ds6.statement = "update xdual_x set x=now()";
    //
    // // 数据源（正确配置）
    // DataSourceConfig ds = new DataSourceConfig();
    // ds.host = "10.20.132.17";
    // ds.port = 3306;
    // ds.schema = "offer1";
    // ds.user = "offer";
    // ds.password = "offer";
    // ds.statement = "update xdual set x=now()";
    //
    // DataNodeConfig node = new DataNodeConfig();
    // node.name = "offer1";
    // node.activedIndex = 0;
    // node.dataSource = new DataSourceConfig[] { ds1, ds2, ds3, ds4, ds5, ds6,
    // ds };
    // return node;
    // }

}