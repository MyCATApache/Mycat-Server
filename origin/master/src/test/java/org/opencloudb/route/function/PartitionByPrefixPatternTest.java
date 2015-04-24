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
package org.opencloudb.route.function;

import junit.framework.Assert;

import org.junit.Test;

public class PartitionByPrefixPatternTest {

	@Test
	public void test()
	{
		/**
		 * ASCII编码：
		 * 48-57=0-9阿拉伯数字
		 * 64、65-90=@、A-Z 
		 * 97-122=a-z
		 * 
		 */
		PartitionByPrefixPattern autoPartition=new PartitionByPrefixPattern();
		autoPartition.setPatternValue(32);
		autoPartition.setPrefixLength(5);
		autoPartition.setMapFile("partition_prefix_pattern.txt");
		autoPartition.init();
		
		String idVal="gf89f9a";
		Assert.assertEquals(true, 0==autoPartition.calculate(idVal)); 
		
		idVal="8df99a";
		Assert.assertEquals(true, 4==autoPartition.calculate(idVal)); 
		
		idVal="8dhdf99a";
		Assert.assertEquals(true, 3==autoPartition.calculate(idVal)); 
	}
}