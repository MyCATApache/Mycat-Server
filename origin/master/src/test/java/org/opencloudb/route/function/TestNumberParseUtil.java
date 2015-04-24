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

import org.junit.Test;

import junit.framework.Assert;

public class TestNumberParseUtil {

	@Test
	public void test() {
		String val = "2000";
		Assert.assertEquals(2000, NumberParseUtil.parseLong(val));
		val = "2M";
		Assert.assertEquals(20000, NumberParseUtil.parseLong(val));
		val = "2M1";
		Assert.assertEquals(20001, NumberParseUtil.parseLong(val));
		val = "1000M";
		Assert.assertEquals(10000000, NumberParseUtil.parseLong(val));
		val = "30K";
		Assert.assertEquals(30000, NumberParseUtil.parseLong(val));
		val = "30K1";
		Assert.assertEquals(30001, NumberParseUtil.parseLong(val));
		val = "30K09";
		Assert.assertEquals(30009, NumberParseUtil.parseLong(val));
	}
}