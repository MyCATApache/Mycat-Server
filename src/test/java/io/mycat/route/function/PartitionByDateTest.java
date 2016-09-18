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

import org.junit.Assert;
import org.junit.Test;

public class PartitionByDateTest {

	@Test
	public void test()  {
		PartitionByDate partition=new PartitionByDate();

		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2014-01-01");
		partition.setsPartionDay("10");
		
		partition.init();
		
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-10"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-01-11"));
		Assert.assertEquals(true, 12 == partition.calculate("2014-05-01"));
		
		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2014-01-01");
		partition.setsEndDate("2014-01-31");
		partition.setsPartionDay("10");
		partition.init();
		
		/**
		 * 0 : 01.01-01.10,02.10-02.19
		 * 1 : 01.11-01.20,02.20-03.01
		 * 2 : 01.21-01.30,03.02-03.12
		 * 3  ： 01.31-02-09,03.13-03.23
		 */
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-10"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-01-11"));
		Assert.assertEquals(true, 3 == partition.calculate("2014-02-01"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-02-19"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-02-20"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-03-01"));
		Assert.assertEquals(true, 2 == partition.calculate("2014-03-02"));
		Assert.assertEquals(true, 2 == partition.calculate("2014-03-11"));
		Assert.assertEquals(true, 3 == partition.calculate("2014-03-20"));


	}
}