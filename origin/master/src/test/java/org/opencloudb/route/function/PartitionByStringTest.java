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

import org.junit.Assert;
import org.junit.Test;

public class PartitionByStringTest {

	@Test
	public void test() {
		PartitionByString rule = new PartitionByString();
		String idVal=null;
		rule.setPartitionLength("512");
		rule.setPartitionCount("2");
		rule.init();
		rule.setHashSlice("0:2");
//		idVal = "0";
//		Assert.assertEquals(true, 0 == rule.calculate(idVal));
//		idVal = "45a";
//		Assert.assertEquals(true, 1 == rule.calculate(idVal));

		
		
		//last 4
		rule = new PartitionByString();
		rule.setPartitionLength("512");
		rule.setPartitionCount("2");
		rule.init();
		//last 4 characters
		rule.setHashSlice("-4:0");
		idVal = "aaaabbb0000";
		Assert.assertEquals(true, 0 == rule.calculate(idVal));
		idVal = "aaaabbb2359";
		Assert.assertEquals(true, 0 == rule.calculate(idVal));
	}
}