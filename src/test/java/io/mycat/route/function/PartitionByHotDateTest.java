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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByHotDateTest {

	@Test
	public void test()  {
PartitionByHotDate partition = new PartitionByHotDate();
		
		partition.setDateFormat("yyyy-MM-dd");
		partition.setsLastDay("10");
		partition.setsPartionDay("1");

		partition.init();
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

		Calendar cDate = Calendar.getInstance();
		cDate.set(Calendar.MONTH, cDate.get(Calendar.MONTH));
		cDate.set(Calendar.DATE, cDate.get(Calendar.DATE));
		Assert.assertEquals(true, 0 == partition.calculate(dateFormat.format(cDate.getTime())));
		
		cDate = Calendar.getInstance();
		cDate.add(Calendar.DATE,-5);
		System.err.println(dateFormat.format(cDate.getTime()));
		Assert.assertEquals(true, 0 == partition.calculate(dateFormat.format(cDate.getTime())));
		
		cDate = Calendar.getInstance();
		cDate.add(Calendar.DATE,-11);
		System.err.println(dateFormat.format(cDate.getTime()));
		Assert.assertEquals(true, 2 == partition.calculate(dateFormat.format(cDate.getTime())));
		
		cDate = Calendar.getInstance();
		cDate.add(Calendar.DATE, -21);
		System.err.println(dateFormat.format(cDate.getTime()));
		Assert.assertEquals(true, 12 == partition.calculate(dateFormat.format(cDate.getTime())));

		cDate = Calendar.getInstance();
		cDate.add(Calendar.DATE,-5);
		System.err.println(dateFormat.format(cDate.getTime()));
		Assert.assertEquals(true, 0 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[0]);
		
		cDate = Calendar.getInstance();
		cDate.add(Calendar.DATE,-11);
		System.err.println(dateFormat.format(cDate.getTime()));
		Assert.assertEquals(true, 0 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[0]);
		Assert.assertEquals(true, 1 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[1]);
		Assert.assertEquals(true, 2 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[2]);

		cDate = Calendar.getInstance();
		cDate.add(Calendar.DATE, -21);
		System.err.println(dateFormat.format(cDate.getTime()));
		Assert.assertEquals(true, 0 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[0]);
		Assert.assertEquals(true, 1 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[1]);
		Assert.assertEquals(true, 2 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[2]);
		Assert.assertEquals(true, 12 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime()))[12]);
		Assert.assertEquals(true, 13 == partition.calculateRange(dateFormat.format(cDate.getTime()),dateFormat.format(Calendar.getInstance().getTime())).length);

	}
}