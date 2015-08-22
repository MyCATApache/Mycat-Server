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
package io.mycat.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	
	private static Calendar cal = Calendar.getInstance();
	private static SimpleDateFormat sdf = new SimpleDateFormat();
	
	public static final String DEFAULT_DATE_PATTERN = "YYYY-MM-dd HH:mm:ss";
	public static final String DATE_PATTERN_FULL = "YYYY-MM-dd HH:mm:ss.SSSSSS";
	public static final String DATE_PATTERN_ONLY_DATE = "YYYY-MM-dd";
	public static final String DEFAULT_TIME_PATTERN = "HHH:mm:ss";
	public static final String TIME_PATTERN_FULL = "HHH:mm:ss.SSSSSS";
	
	public static Date parseDate(String dateStr) throws ParseException {
		return parseDate(dateStr, DEFAULT_DATE_PATTERN);
	}
	
	public static Date parseDate(String dateStr, String datePattern) throws ParseException {
		sdf.applyPattern(datePattern);
		return sdf.parse(dateStr);
	}
	
	public static int getYear(Date date) {
		cal.setTime(date);
		return cal.get(Calendar.YEAR);
	}
	
	public static int getMonth(Date date) {
		cal.setTime(date);
		return cal.get(Calendar.MONTH);
	}
	
	public static int getDay(Date date) {
		cal.setTime(date);
		return cal.get(Calendar.DATE);
	}
	
	public static int getHour(Date date) {
		cal.setTime(date);
		return cal.get(Calendar.HOUR_OF_DAY);
	}
	
	public static int getMinute(Date date) {
		cal.setTime(date);
		return cal.get(Calendar.MINUTE);
	}
	
	public static int getSecond(Date date) {
		cal.setTime(date);
		return cal.get(Calendar.SECOND);
	}
	
	public static int getMicroSecond(Date date) {
		cal.setTime(date);
		return cal.get(Calendar.MILLISECOND);
	}

}
