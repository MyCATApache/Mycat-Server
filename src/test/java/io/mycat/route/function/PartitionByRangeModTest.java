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
package io.mycat.route.function;

import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.config.loader.ConfigInitializer;
import io.mycat.server.config.node.RuleConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;

import java.math.BigInteger;
import java.sql.SQLNonTransientException;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

public class PartitionByRangeModTest
{

    @Test
    public void test()
    {
    	ConfigInitializer confInit = new ConfigInitializer(true);
        Map<String, RuleConfig> ruleConfigs = confInit.getTableRules();
        Set<String> sets = ruleConfigs.keySet();
        for(String ruleStr : sets){
        	if(ruleConfigs.get(ruleStr).getFunctionName().indexOf("PartitionByRangeMod")!=-1){
        		AbstractPartitionAlgorithm autoPartition = ruleConfigs.get(ruleStr).getRuleAlgorithm();
        		autoPartition.init();
                String idVal = "0";
                Assert.assertEquals(true, 0 == autoPartition.calculate(idVal));
                idVal = "1";
                Assert.assertEquals(true, 1 == autoPartition.calculate(idVal));
                idVal = "2";
                Assert.assertEquals(true, 2 == autoPartition.calculate(idVal));
                idVal = "3";
                Assert.assertEquals(true, 3 == autoPartition.calculate(idVal));
                idVal = "4";
                Assert.assertEquals(true, 4 == autoPartition.calculate(idVal));
                idVal = "5";
                Assert.assertEquals(true, 0 == autoPartition.calculate(idVal));

                idVal="2000000";
        		Assert.assertEquals(true, 0==autoPartition.calculate(idVal));

        		idVal="2000001";
        		Assert.assertEquals(true, 5==autoPartition.calculate(idVal));

        		idVal="4000000";
        		Assert.assertEquals(true, 5==autoPartition.calculate(idVal));

        		idVal="4000001";
        		Assert.assertEquals(true, 7==autoPartition.calculate(idVal));
        	}

        }
    }


    private static int mod(long v, int size)
    {
        BigInteger bigNum = BigInteger.valueOf(v).abs();
        return (bigNum.mod(BigInteger.valueOf(size))).intValue();
    }

    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

    public PartitionByRangeModTest() {
    	ConfigInitializer confInit = new ConfigInitializer(true);
        schemaMap = confInit.getSchemas();
    }

    @Test
    public void testRange() throws SQLNonTransientException
    {
        String sql = "select * from offer  where id between 2000000  and 4000001     order by id desc limit 100";
        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(10, rrs.getNodes().length);

        sql = "select * from offer  where id between 9  and 2000     order by id desc limit 100";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(5, rrs.getNodes().length);

        sql = "select * from offer  where id between 4000001  and 6005001     order by id desc limit 100";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(8, rrs.getNodes().length);


    }
}