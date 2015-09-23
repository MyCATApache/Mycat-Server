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

import io.mycat.server.config.loader.ConfigInitializer;
import io.mycat.server.config.node.RuleConfig;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByPatternTest {

	@Test
	public void test() {
		ConfigInitializer confInit = new ConfigInitializer(true);
        Map<String, RuleConfig> ruleConfigs = confInit.getTableRules();
        Set<String> sets = ruleConfigs.keySet();
        for(String ruleStr : sets){
        	if(ruleConfigs.get(ruleStr).getFunctionName().indexOf("PartitionByPattern")!=-1){
        		AbstractPartitionAlgorithm autoPartition = ruleConfigs.get(ruleStr).getRuleAlgorithm();
        		//autoPartition.setPatternValue(256);
        		//autoPartition.setDefaultNode(2);
        		autoPartition.init();
        		String idVal = "0";
        		Assert.assertEquals(true, 7 == autoPartition.calculate(idVal));
        		idVal = "45a";
        		Assert.assertEquals(true, 2 == autoPartition.calculate(idVal));
        	}

        }
	}
}