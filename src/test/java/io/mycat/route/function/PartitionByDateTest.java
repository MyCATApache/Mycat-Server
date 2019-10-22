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
//
//		/**
//		 * 0 : 01.01-01.10,02.10-02.19
//		 * 1 : 01.11-01.20,02.20-03.01
//		 * 2 : 01.21-01.30,03.02-03.12
//		 * 3  ： 01.31-02-09,03.13-03.23
//		 */
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

		//测试默认1
		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2014-01-01");
		partition.setsEndDate("2014-01-31");
		partition.setsPartionDay("1");
		partition.init();
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 9 == partition.calculate("2014-01-10"));
		Assert.assertEquals(true, 10 == partition.calculate("2014-01-11"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-02-01"));
		System.out.println(partition.calculate("2014-02-19"));


		//自然日测试
		//1、只开启自然日分表开关
		PartitionByDate partition2=new PartitionByDate();
		partition2.setDateFormat("yyyy-MM-dd");
		partition2.setsNaturalDay("1");
		partition2.init();
		//Assert.assertEquals(true, 6 == partition2.calculate("2014-01-20"));
		Assert.assertEquals(true, 19 == partition2.calculate("2014-01-20"));
		Assert.assertEquals(true, 0 == partition2.calculate("2014-03-01"));
		Assert.assertEquals(true, 30 == partition2.calculate("2018-03-31"));


		//2、顺便开启开始时间
		partition2.setDateFormat("yyyy-MM-dd");
		partition2.setsNaturalDay("1");
		partition2.setsPartionDay("1");
		partition2.setsBeginDate("2014-01-02");
		partition2.init();

		Assert.assertEquals(true, 19 == partition2.calculate("2014-01-20"));
		Assert.assertEquals(true, 0 == partition2.calculate("2014-03-01"));
		Assert.assertEquals(true, 30 == partition2.calculate("2018-03-31"));

		//2、顺便开启开始时间,结束时间不足28天（开启自然日失败，默认间隔模式）PartionDay=1
		PartitionByDate partition3=new PartitionByDate();
		partition3.setDateFormat("yyyy-MM-dd");
		partition3.setsNaturalDay("1");
		partition3.setsPartionDay("1");
		partition3.setsBeginDate("2014-01-02");
		partition3.setsEndDate("2014-01-20");
		partition3.init();
		Assert.assertEquals(true, 0 == partition3.calculate("2014-01-02"));
		Assert.assertEquals(true, 1 == partition3.calculate("2014-01-03"));
		Assert.assertEquals(true, 2 == partition3.calculate("2014-01-04"));
		Assert.assertEquals(true, 6 == partition3.calculate("2014-01-08"));
		Assert.assertEquals(true, 8 == partition3.calculate("2014-01-10"));
		Assert.assertEquals(true, 12 == partition3.calculate("2014-01-14"));
		Assert.assertEquals(true, 18 == partition3.calculate("2014-01-20"));
		System.out.println(partition3.calculate("2014-03-01"));
		//Assert.assertEquals(true, 0 == partition3.calculate("2014-03-01"));

		//3、顺便开启开始时间,结束时间不足28天（开启自然日失败，默认间隔模式）PartionDay=10 恢复间隔模式
		partition.setsNaturalDay("1");
		partition.setsBeginDate("2014-01-01");
		partition.setsEndDate("2014-01-24");
		partition.setsPartionDay("10");
		partition.init();
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-10"));
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-05"));
		Assert.assertEquals(true, 1 == partition.calculate("2014-01-20"));
		System.out.println("------------success!----");


		//4、顺便开启开始时间,结束时间超过29天 PartionDay=1
		partition.setsNaturalDay("1");
		partition.setsBeginDate("2014-01-01");
		partition.setsEndDate("2014-01-29");
		partition.setsPartionDay("10");
		partition.init();
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 9 == partition.calculate("2014-01-10"));
		Assert.assertEquals(true, 4 == partition.calculate("2014-01-05"));
		Assert.assertEquals(true, 19 == partition.calculate("2014-01-20"));
		Assert.assertEquals(true, 30 == partition.calculate("2018-01-31"));


		//4、顺便开启开始时间,结束时间超过29天 PartionDay=1
		partition.setsNaturalDay("1");
		partition.setsBeginDate("2014-01-01");
		partition.setsEndDate("2018-01-29");
		partition.setsPartionDay("1");
		partition.init();
		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		Assert.assertEquals(true, 9 == partition.calculate("2014-01-10"));
		Assert.assertEquals(true, 4 == partition.calculate("2014-01-05"));
		Assert.assertEquals(true, 19 == partition.calculate("2014-01-20"));
		Assert.assertEquals(true, 30 == partition.calculate("2018-01-31"));
		System.out.println("------------success!----");

	}
}