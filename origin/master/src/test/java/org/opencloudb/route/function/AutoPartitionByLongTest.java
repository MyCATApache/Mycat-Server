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

public class AutoPartitionByLongTest {

	@Test
	public void test()
	{
		AutoPartitionByLong autoPartition=new AutoPartitionByLong();
		autoPartition.setMapFile("autopartition-long.txt");
		autoPartition.init();
		String idVal="0";
		Assert.assertEquals(true, 0==autoPartition.calculate(idVal)); 
		
		idVal="2000000";
		Assert.assertEquals(true, 0==autoPartition.calculate(idVal)); 
		
		idVal="2000001";
		Assert.assertEquals(true, 1==autoPartition.calculate(idVal)); 
		
		idVal="4000000";
		Assert.assertEquals(true, 1==autoPartition.calculate(idVal)); 
		
		idVal="4000001";
		Assert.assertEquals(true, 2==autoPartition.calculate(idVal)); 
	}
}