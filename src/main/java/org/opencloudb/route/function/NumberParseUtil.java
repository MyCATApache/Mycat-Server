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

public class NumberParseUtil {
	/**
	 * 只去除开头结尾的引号，而且是结对去除，语法不对的话通不过
	 * @param number
	 * @return
	 */
	public static String eliminateQoute(String number){
		number = number.trim();
		if(number.contains("\"")){
			if(number.charAt(0)=='\"'){
				number = number.substring(1);
				if(number.charAt(number.length()-1)=='\"'){
					number = number.substring(0,number.length()-1);
				}
			}
		}else if(number.contains("\'")){
			if(number.charAt(0)=='\''){
				number = number.substring(1);
				if(number.charAt(number.length()-1)=='\''){
					number = number.substring(0,number.length()-1);
				}
			}
		}
		return number;
	}
	/**
	 * can parse values like 200M ,200K,200M1(2000001)
	 * 
	 * @param val
	 * @return
	 */
	public static long parseLong(String val) {
		val = val.toUpperCase();
		int indx = val.indexOf("M");

		int plus = 10000;
		if (indx < 0) {
			indx = val.indexOf("K");
			plus = 1000;
		}
		if (indx > 0) {
			String longVal = val.substring(0, indx);

			long theVale = Long.parseLong(longVal) * plus;
			String remain = val.substring(indx + 1);
			if (remain.length() > 0) {
				theVale += Integer.parseInt(remain);
			}
			return theVale;
		} else {
			return Long.parseLong(val);
		}

	}
}