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

import java.util.Arrays;

public class PartitionByMonthTest {

	@Test
	public void test()  {
		PartitionByMonth partition = new PartitionByMonth();

		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2014-01-01");

		partition.init();

		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-10"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-31"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-02-01"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-02-28"));
		Assert.assertEquals(true, 2 == partition.calculate("2014-03-1"));
		Assert.assertEquals(true, 11 == partition.calculate("2014-12-31"));
		Assert.assertEquals(true, 12 == partition.calculate("2015-01-31"));
		Assert.assertEquals(true, 23 == partition.calculate("2015-12-31"));

		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2015-01-01");
		partition.setsEndDate("2015-12-01");

		partition.init();

		/**
		 *  0 : 2016-01-01~31, 2015-01-01~31, 2014-01-01~31
		 *  1 : 2016-02-01~28, 2015-02-01~28, 2014-02-01~28
		 *  5 : 2016-06-01~30, 2015-06-01~30, 2014-06-01~30
		 * 11 : 2016-12-01~31, 2015-12-01~31, 2014-12-01~31
		 */

		Assert.assertEquals(true, 0 == partition.calculate("2013-01-02"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 0 == partition.calculate("2015-01-10"));
		Assert.assertEquals(true, 0 == partition.calculate("2015-01-31"));
		Assert.assertEquals(true, 0 == partition.calculate("2016-01-20"));

		Assert.assertEquals(true, 1 == partition.calculate("2013-02-02"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-02-01"));
		Assert.assertEquals(true, 1 == partition.calculate("2015-02-10"));
		Assert.assertEquals(true, 1 == partition.calculate("2015-02-28"));
		Assert.assertEquals(true, 1 == partition.calculate("2016-02-20"));

		Assert.assertEquals(true, 5 == partition.calculate("2013-06-01"));
		Assert.assertEquals(true, 5 == partition.calculate("2014-06-01"));
		Assert.assertEquals(true, 5 == partition.calculate("2015-06-10"));
		Assert.assertEquals(true, 5 == partition.calculate("2015-06-28"));
		Assert.assertEquals(true, 5 == partition.calculate("2016-06-20"));

		Assert.assertEquals(true, 11 == partition.calculate("2013-12-28"));
		Assert.assertEquals(true, 11 == partition.calculate("2014-12-01"));
		Assert.assertEquals(true, 11 == partition.calculate("2014-12-31"));
		Assert.assertEquals(true, 11 == partition.calculate("2015-12-11"));
		Assert.assertEquals(true, 11 == partition.calculate("2016-12-31"));

	}

	/**
	 * 范围对比
	 */
	@Test
	public void sence1CalculateRangeContrastTest(){
		// 场景1：无开始/结束时间，节点数量必须是12个，从1月~12月
		PartitionByMonth partition = new PartitionByMonth();
		partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2013-01-01");
        partition.setsEndDate("2013-12-01");
		partition.init();

		PartitionByMonth scene = new PartitionByMonth();
		scene.setDateFormat("yyyy-MM-dd");
		scene.init();
		Assert.assertEquals(
				Arrays.toString(partition.calculateRange("2014-01-01", "2014-04-03")),
				Arrays.toString(scene.calculateRange("2014-01-01", "2014-04-03"))
		);
		Assert.assertEquals(
				Arrays.toString(partition.calculateRange("2013-01-01", "2014-04-03")),
				Arrays.toString(scene.calculateRange("2013-01-01", "2014-04-03"))
		);
		Assert.assertEquals(
				// []
				Arrays.toString(partition.calculateRange("2015-01-01", "2014-04-03")),
				// []
				Arrays.toString(scene.calculateRange("2015-01-01", "2014-04-03"))
		);
	}
	@Test
	public void sence1(){
		PartitionByMonth scene = new PartitionByMonth();
		scene.setDateFormat("yyyy-MM-dd");
		scene.init();

		Assert.assertEquals(true, 0 == scene.calculate("2014-01-01"));
		Assert.assertEquals(true, 0 == scene.calculate("2014-01-10"));
		Assert.assertEquals(true, 0 == scene.calculate("2014-01-31"));
		Assert.assertEquals(true, 1 == scene.calculate("2014-02-01"));
		Assert.assertEquals(true, 1 == scene.calculate("2014-02-28"));
		Assert.assertEquals(true, 2 == scene.calculate("2014-03-1"));
		Assert.assertEquals(true, 11 == scene.calculate("2014-12-31"));
		Assert.assertEquals(true, 0 == scene.calculate("2015-01-31"));
		Assert.assertEquals(true, 11 == scene.calculate("2015-12-31"));
	}
}
