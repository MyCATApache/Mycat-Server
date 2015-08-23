
package io.mycat.route.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.DateUtils;

/**
 *
 * @author yan.yan@huawei.com
 */
public final class DateUtil extends DateUtils {

    /** yyyyMMdd */
    public final static String SHORT_FORMAT           = "yyyyMMdd";

    /** yyyyMMddHHmmss */
    public final static String LONG_FORMAT            = "yyyyMMddHHmmss";

    /** yyyy-MM-dd */
    public final static String WEB_FORMAT             = "yyyy-MM-dd";

    /** HHmmss */
    public final static String TIME_FORMAT            = "HHmmss";

    /** yyyyMM */
    public final static String MONTH_FORMAT           = "yyyyMM";

    /** yyyy年MM月dd�? */
    public final static String CHINA_FORMAT           = "yyyy年MM月dd�?";

    /** yyyy-MM-dd HH:mm:ss */
    public final static String LONG_WEB_FORMAT        = "yyyy-MM-dd HH:mm:ss";

    /** yyyy-MM-dd HH:mm */
    public final static String LONG_WEB_FORMAT_NO_SEC = "yyyy-MM-dd HH:mm";

    /**
     * 日期对象解析成日期字符串基础方法，可以据此封装出多种便捷的方法直接使�?
     * 
     * @param date 待格式化的日期对�?
     * @param format 输出的格�?
     * @return 格式化的字符�?
     */
    public static String format(Date date, String format) {
        if (date == null || StringUtil.isBlank(format)) {
            return StringUtil.EMPTY;
        }

        return new SimpleDateFormat(format, Locale.SIMPLIFIED_CHINESE).format(date);
    }

    /**
     * 格式化当前时�?
     * 
     * @param format 输出的格�?
     * @return
     */
    public static String formatCurrent(String format) {
        if (StringUtil.isBlank(format)) {
            return StringUtil.EMPTY;
        }

        return format(new Date(), format);
    }

    /**
     * 日期字符串解析成日期对象基础方法，可以在此封装出多种便捷的方法直接使�?
     * 
     * @param dateStr 日期字符�?
     * @param format 输入的格�?
     * @return 日期对象
     * @throws ParseException 
     */
    public static Date parse(String dateStr, String format) throws ParseException {
        if (StringUtil.isBlank(format)) {
            throw new ParseException("format can not be null.", 0);
        }

        if (dateStr == null || dateStr.length() < format.length()) {
            throw new ParseException("date string's length is too small.", 0);
        }

        return new SimpleDateFormat(format, Locale.SIMPLIFIED_CHINESE).parse(dateStr);
    }

    /**
     * 日期字符串格式化基础方法，可以在此封装出多种便捷的方法直接使�?
     * 
     * @param dateStr 日期字符�?
     * @param formatIn 输入的日期字符串的格�?
     * @param formatOut 输出日期字符串的格式
     * @return 已经格式化的字符�?
     * @throws ParseException
     */
    public static String format(String dateStr, String formatIn, String formatOut)
                                                                                  throws ParseException {

        Date date = parse(dateStr, formatIn);
        return format(date, formatOut);
    }

    /**
     * 把日期对象按�?<code>yyyyMMdd</code>格式解析成字符串
     * 
     * @param date 待格式化的日期对�? 
     * @return 格式化的字符�?
     */
    public static String formatShort(Date date) {
        return format(date, SHORT_FORMAT);
    }

    /**
     * 把日期字符串按照<code>yyyyMMdd</code>格式，进行格式化
     * 
     * @param dateStr 待格式化的日期字符串
     * @param formatIn 输入的日期字符串的格�? 
     * @return 格式化的字符�?
     */
    public static String formatShort(String dateStr, String formatIn) throws ParseException {
        return format(dateStr, formatIn, SHORT_FORMAT);
    }

    /**
     * 把日期对象按�?<code>yyyy-MM-dd</code>格式解析成字符串
     * 
     * @param date 待格式化的日期对�? 
     * @return 格式化的字符�?
     */
    public static String formatWeb(Date date) {
        return format(date, WEB_FORMAT);
    }

    /**
     * 把日期字符串按照<code>yyyy-MM-dd</code>格式，进行格式化
     * 
     * @param dateStr 待格式化的日期字符串
     * @param formatIn 输入的日期字符串的格�? 
     * @return 格式化的字符�?
     * @throws ParseException 
     */
    public static String formatWeb(String dateStr, String formatIn) throws ParseException {
        return format(dateStr, formatIn, WEB_FORMAT);
    }

    /**
     * 把日期对象按�?<code>yyyyMM</code>格式解析成字符串
     * 
     * @param date 待格式化的日期对�? 
     * @return 格式化的字符�?
     */
    public static String formatMonth(Date date) {

        return format(date, MONTH_FORMAT);
    }

    /**
     * 把日期对象按�?<code>HHmmss</code>格式解析成字符串
     * 
     * @param date 待格式化的日期对�? 
     * @return 格式化的字符�?
     */
    public static String formatTime(Date date) {
        return format(date, TIME_FORMAT);
    }

    /**
     * 获取yyyyMMddHHmmss+n位随机数格式的时间戳
     * 
     * @param n 随机数位�?
     * @return
     */
    public static String getTimestamp(int n) {
        return formatCurrent(LONG_FORMAT) + RandomStringUtils.randomNumeric(n);
    }

    /**
     * 根据日期格式返回昨日日期
     * 
     * @param format 日期格式
     * @return
     */
    public static String getYesterdayDate(String format) {
        return getDateCompareToday(format, -1, 0);
    }

    /**
     * 把当日日期作为基准，按照格式返回相差�?定间隔的日期
     *
     * @param format 日期格式
     * @param daysAfter 和当日比相差几天，例�?3代表3天后�?-1代表1天前
     * @param monthAfter 和当日比相差几月，例�?2代表2月后�?-3代表3月前
     * @return
     */
    public static String getDateCompareToday(String format, int daysAfter, int monthAfter) {
        Calendar today = Calendar.getInstance();
        if (daysAfter != 0) {
            today.add(Calendar.DATE, daysAfter);
        }
        if (monthAfter != 0) {
            today.add(Calendar.MONTH, monthAfter);
        }
        return format(today.getTime(), format);
    }

    /**
     * 根据日期格式返回上月的日�?
     * 
     * @param format
     * @return
     */
    public static String getLastMonth(String format) {
        Calendar today = Calendar.getInstance();
        today.add(Calendar.MONTH, -1);
        return format(today.getTime(), format);
    }

    /**
     * 平移当前时间，以分为单元，minutes
     * 
     * @param minutes
     * @return
     */
    public static Date addCurMin(long minutes) {
        return DateUtils.addMinutes(new Date(), (int) minutes);
    }

    /**
     * 平移当前时间，以秒为单元，minutes
     * 
     * @param secs
     * @return
     */
    public static Date addCurSeconds(long secs) {
        return addSeconds(new Date(), (int) secs);
    }
}
