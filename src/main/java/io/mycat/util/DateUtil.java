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
