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

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByDateTest {

	@Test
	public void test() throws ParseException {
		PartitionByDate partition=new PartitionByDate();

		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2014-01-01");
		partition.setsPartionDay("10");
		partition.setNodesText("0,1,2,3,4;5,6,7");
		partition.setGroupMode("true");
		partition.init();
		
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
		int[][] nodes=new int[][]{{0,1,2,3,4},{5,6,7}};

		Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
		
		long millis=format.parse("2014-01-10").getTime();
		Assert.assertEquals(true,  nodes[0][(int)(millis%5)]== partition.calculate("2014-01-10"));
		
		millis=format.parse("2014-01-11").getTime();
		Assert.assertEquals(true, nodes[1][(int)(millis%3)] == partition.calculate("2014-01-11"));
		
		millis=format.parse("2014-05-01").getTime();
		long minus=((millis-format.parse("2014-01-01").getTime())/10*24*60*60*1000);
		Assert.assertEquals(true,minus%2==0);
		Assert.assertEquals(true, nodes[(int)(minus%2)][(int)(millis%5)] == partition.calculate("2014-05-01"));
	}
}