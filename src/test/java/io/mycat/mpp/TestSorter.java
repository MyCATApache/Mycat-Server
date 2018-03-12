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
package io.mycat.mpp;

import org.junit.Assert;
import org.junit.Test;

import io.mycat.util.ByteUtil;

public class TestSorter {

	@Test
	public void testDecimal() {
		String d1 = "-1223.000";
		byte[] d1b = d1.getBytes();
		Assert.assertEquals(true, -1223.0 == ByteUtil.getDouble(d1b));
		d1b = "-99999.890".getBytes();
		Assert.assertEquals(true, -99999.890 == ByteUtil.getDouble(d1b));
		// 221346.000
		byte[] data2 = new byte[] { 50, 50, 49, 51, 52, 54, 46, 48, 48, 48 };
		Assert.assertEquals(true, 221346.000 == ByteUtil.getDouble(data2));
		// 1234567890
		byte[] data3 = new byte[] { 49, 50, 51, 52, 53, 54, 55, 56, 57, 48 };
		Assert.assertEquals(true, 1234567890 == ByteUtil.getInt(data3));

		// 0123456789
		byte[] data4 = new byte[] { 48, 49, 50, 51, 52, 53, 54, 55, 56, 57 };
		Assert.assertEquals(true, 123456789 == ByteUtil.getInt(data4));
	}

	@Test
	public void testNumberCompare() {
		byte[] b1 = "0".getBytes();
		byte[] b2 = "0".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) == 0);

		b1 = "0".getBytes();
		b2 = "1".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)< 0);
		
		b1 = "10".getBytes();
		b2 = "1".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)> 0);
		
		b1 = "100.0".getBytes();
		b2 = "100.0".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)==0);
		
		b1 = "100.000".getBytes();
		b2 = "100.0".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)>0);
		
		b1 = "-100.000".getBytes();
		b2 = "-100.0".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)<0);
		
		b1 = "-100.001".getBytes();
		b2 = "-100.0".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)<0);
		
		b1 = "-100.001".getBytes();
		b2 = "100.0".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)<0);
		
		b1 = "90".getBytes();
		b2 = "10000".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)<0);
		b1 = "-90".getBytes();
		b2 = "-10000".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)>0);
		
		b1 = "98".getBytes();
		b2 = "98000".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)<0);
		
		b1 = "-98".getBytes();
		b2= "-98000".getBytes();
		Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)>0);
		
		b1="12002585786".getBytes();
        b2="12002585785".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2)>0);

	}
}