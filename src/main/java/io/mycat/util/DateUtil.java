package io.mycat.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.text.ParseException;
import java.util.Date;

/**
 * 使用joda解析date,可以得到date的year,month,day等字段值
 * @author CrazyPig
 *
 */
public class DateUtil {
	
	
	public static final String DEFAULT_DATE_PATTERN = "YYYY-MM-dd HH:mm:ss";
	public static final String DATE_PATTERN_FULL = "YYYY-MM-dd HH:mm:ss.SSSSSS";
	public static final String DATE_PATTERN_ONLY_DATE = "YYYY-MM-dd";
	public static final String DEFAULT_TIME_PATTERN = "HHH:mm:ss";
	public static final String TIME_PATTERN_FULL = "HHH:mm:ss.SSSSSS";
	
	/**
	 * 根据日期字符串解析得到date类型日期
	 * @param dateStr
	 * @return
	 * @throws ParseException
	 */
	public static Date parseDate(String dateStr) throws ParseException {
		return parseDate(dateStr, DEFAULT_DATE_PATTERN);
	}
	
	/**
	 * 根据日期字符串和日期格式解析得到date类型日期
	 * @param dateStr
	 * @param datePattern
	 * @return
	 * @throws ParseException
	 */
	public static Date parseDate(String dateStr, String datePattern) throws ParseException {
		DateTime dt = DateTimeFormat.forPattern(datePattern).parseDateTime(dateStr);
		return dt.toDate();
	}
	
	/**
	 * 获取date对象年份
	 * @param date
	 * @return
	 */
	public static int getYear(Date date) {
		DateTime dt = new DateTime(date);
		return dt.getYear();
	}
	
	/**
	 * 获取date对象月份
	 * @param date
	 * @return
	 */
	public static int getMonth(Date date) {
		DateTime dt = new DateTime(date);
		return dt.getMonthOfYear();
	}
	
	/**
	 * 获取date对象天数
	 * @param date
	 * @return
	 */
	public static int getDay(Date date) {
		DateTime dt = new DateTime(date);
		return dt.getDayOfMonth();
	}
	
	/**
	 * 获取date对象小时数
	 * @param date
	 * @return
	 */
	public static int getHour(Date date) {
		DateTime dt = new DateTime(date);
		return dt.getHourOfDay();
	}
	
	/**
	 * 获取date对象分钟数
	 * @param date
	 * @return
	 */
	public static int getMinute(Date date) {
		DateTime dt = new DateTime(date);
		return dt.getMinuteOfHour();
	}
	
	/**
	 * 获取date对象秒数
	 * @param date
	 * @return
	 */
	public static int getSecond(Date date) {
		DateTime dt = new DateTime(date);
		return dt.getSecondOfMinute();
	}
	
	/**
	 * 获取date对象毫秒数
	 * @param date
	 * @return
	 */
	public static int getMicroSecond(Date date) {
		DateTime dt = new DateTime(date);
		return dt.getMillisOfSecond();
	}

}