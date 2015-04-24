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
package org.opencloudb.route.perf;

import java.sql.SQLNonTransientException;

import org.opencloudb.SimpleCachePool;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.factory.RouteStrategyFactory;

/**
 * @author mycat
 */
public class ShardingMultiTableSpace {
    private SchemaConfig schema;
    private static int total=1000000;
    protected LayerCachePool cachePool = new SimpleCachePool();
    public ShardingMultiTableSpace() throws InterruptedException {
         String schemaFile = "/route/schema.xml";
 		String ruleFile = "/route/rule.xml";
 		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
 		schema = schemaLoader.getSchemas().get("cndb");
    }

    /**
     * 路由到tableSpace的性能测试
     * 
     * @throws SQLNonTransientException
     */
    public void testTableSpace() throws SQLNonTransientException {
        SchemaConfig schema = getSchema();
        String sql = "select id,member_id,gmt_create from offer where member_id in ('1','22','333','1124','4525')";
        for (int i = 0; i < total; i++) {
            RouteStrategyFactory.getRouteStrategy().route(new SystemConfig(),schema, -1,sql, null, null,cachePool);
        }
    }

    protected SchemaConfig getSchema() {
        return schema;
    }

    public static void main(String[] args) throws Exception {
        ShardingMultiTableSpace test = new ShardingMultiTableSpace();
        System.currentTimeMillis();

        long start = System.currentTimeMillis();
        test.testTableSpace();
        long end = System.currentTimeMillis();
        System.out.println("take " + (end - start) + " ms. avg "+(end-start+0.0)/total);
    }
}