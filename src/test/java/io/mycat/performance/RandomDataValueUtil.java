package io.mycat.performance;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * genarate random test data 
 * such as 
 * values ('$date{yyyyMMddHHmmsss-[2014-2015]y}/psn$date{yyyy}s/$int(0-9999)/16767:20725','$char(2,0-99) OPP_$enum(BJ,SH,GZ,SZ)_$int(0-9)',$int(10,11),$int(400,420,500,600,800),$int(0-1000),$int(0-100),Sint(0-10),$int(0-99),'201408040028317067b41c0db-4a93-4360-9eb4-e159d1dbef45',$phone,2,2014071715,2315998,1397,152317998,1395,'0000');
 * @author wuzhih
 *
 */
/**
 * 
 * @author wuzhih
 * 
 */
public class RandomDataValueUtil {

	/**
	 * eval template contains random vars and replace them with real value alues
	 * (
	 * '${date(yyyyMMddHHmmsss-[2014-2015]y)}/psn${date(yyyy)}s/${int(0-9999)}/1
	 * 6 7 6 7 : 2 0 7 2 5 ' , ' $ { s t r i n g ( 2 , 0 - 9 9 )}
	 * OPP_${enum(BJ,SH,GZ,SZ)}_${int(0-9)}
	 * ',${int(10,11)},$int(400,420,500,600,800),$int(0-1000),$int(0-100),$int(0-10),$int(0-99),'201408040028317067b41c0db-4a93-4360-9eb4-e159d1
	 * d b e f 4 5 ' , $ p h o n e , 2 , 2 0 1 4 0 7 1 7 1 5 , 2 3 1 5 9 9 8 , 1
	 * 3 9 7 , 1 5 2 3 1 7 9 9 8 , 1 3 9 5 , ' 0 0 0 0 ' )
	 * 
	 * @param templateStr
	 * @return
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static LinkedList<StringItem> parselRandVarTemplateString(
			String templateStr) throws Exception {
		char[] chars = templateStr.toCharArray();
		LinkedList<StringItem> stringItems = new LinkedList<StringItem>();
		int curPos = 0;
		int prevPattenEndPos = 0;
		while (curPos < chars.length) {
			char c = chars[curPos];
			if (c == '$' && curPos + 1 < chars.length
					&& chars[curPos + 1] == '{') {
				int start = curPos;
				curPos += 2;
				int end = -1;
				if (curPos < chars.length) {
					for (int i = curPos; i < chars.length; i++) {
						if (chars[i] == '}') {
							end = i;
							// found pattern
							if (prevPattenEndPos < start) {// some constant
															// string chars
								StringItem item = new StringItem();
								item.initString(templateStr.substring(
										prevPattenEndPos, start));
								stringItems.add(item);
							}
							// add variable pattern item
							stringItems.add(StringItemFactory
									.parseVarPattern(templateStr.substring(
											curPos, end)));
							prevPattenEndPos = end + 1;
							curPos = end + 1;
							break;
						}
					}
					if (end == -1) {
						// not found pattern end
						throw new RuntimeException(
								"can't find var patten end pos ,start at "
										+ start);
					}
				} else {
					curPos++;
				}

			} else {
				curPos++;
			}
		}
		// add last
		if (prevPattenEndPos < templateStr.length()) {
			StringItem item = new StringItem();
			item.initString(templateStr.substring(prevPattenEndPos,
					templateStr.length()));
			stringItems.add(item);
		}
		return stringItems;
	}

	public static Properties loadFromPropertyFile(String sqlFile)
			throws IOException {
		java.util.Properties pros = new Properties();
		FileInputStream fin = null;
		fin = new FileInputStream(sqlFile);
		pros.load(fin);
		fin.close();
		return pros;
	}

	public static String evalRandValueString(LinkedList<StringItem> items) {
		StringBuilder sb = new StringBuilder();
		for (StringItem item : items) {
			sb.append(item.getValue());
		}
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		String sqlTemplate = "insert into opp_call (logthread, instanceid,callresult,partner, app_id,api_id,apiversion,format,token , phone,calltype, calldate,callminutes,callcost,ecipcalltime,ecipcallcost ,respcode) values ('${date(yyyyMMddHHmmssSSS-[2014-2015]y)}/psn2002s/${int(0-9999)}/${int(1111-9999)}:20725','${char([0-9]2:2)} OPP_${enum(BJ,SH,WU,GZ)}_1',10,${int(10-999)},${int(10-99)},100,3,15,'${date(yyyyMMddHHmmssSSS-[2014-2015]y}${char([a-f,0-9]8:8)}-${char([a-f,0-9]4:4)}-${char([0-9]4:4)}-9eb4-${char([a-f,0-9]12:12)}',${phone(139-189)},2,${date(yyyyMMddHH-[2014-2015]y},2315998,1397,${date(HHmmssSSS)},${int(100-1000)},'${enum(0000,0001,0002)}');";
		System.out.println("SQL template:\r\n" + sqlTemplate);
		LinkedList<StringItem> allItems = parselRandVarTemplateString(sqlTemplate);
		// for (StringItem item : allItems) {
		// System.out.println(item);
		// }
		System.out.println("Random SQLs ");
		int total = 5;
		for (int i = 0; i < total; i++) {
			System.out.println(evalRandValueString(allItems));
		}
	}
}

class StringItemFactory {
	private static final Map<String, Class<? extends StringItem>> strItemsMap = new HashMap<String, Class<? extends StringItem>>();
	static {
		strItemsMap.put("date", DateVarItem.class);
		strItemsMap.put("int", IntVarItem.class);
		strItemsMap.put("char", CharVarItem.class);
		strItemsMap.put("enum", EnumVarItem.class);
		strItemsMap.put("phone", PhoneVarItem.class);

	}

	public static StringItem parseVarPattern(String content)
			throws InstantiationException, IllegalAccessException {
		String name = content.substring(0, content.indexOf('('));
		Class<? extends StringItem> cls = strItemsMap.get(name);
		if (cls == null) {
			throw new RuntimeException("not find var type of  " + name);
		}
		StringItem obj = cls.newInstance();
		obj.initString(content);
		return obj;

	}

}

class PhoneVarItem extends StringItem {

	//
	long[] rang = { 13900000000L, 19900000000L };

	public void initString(String content) {
		int start = content.indexOf('(');
		int end = content.indexOf(')');
		String range = content.substring(start + 1, end);
		String[] items = range.split("-");
		rang[0] = Long.valueOf(patchLenth(items[0], 11 - items[0].length()));
		rang[1] = Long.valueOf(patchLenth(items[1], 11 - items[1].length()));
	}

	public static String patchLenth(String origin, int patchlen) {
		StringBuffer sb = new StringBuffer();
		sb.append(origin);
		for (int i = 0; i < patchlen; i++) {
			sb.append('0');
		}
		return sb.toString();
	}

	public String getValue() {
		long span = rang[1] - rang[0] + 1;
		return Math.abs(rand.nextInt()) % span + rang[0] + "";
	}

	@Override
	public String toString() {
		return "PhoneVarItem [rang=" + Arrays.toString(rang) + "]";
	}

}

class EnumVarItem extends StringItem {
	// {enum(BJ,SH,GZ,SZ)
	String[] enums = {};

	public void initString(String content) {
		int start = content.indexOf('(');
		int end = content.indexOf(')');
		String range = content.substring(start + 1, end);
		String[] items = range.split(",");
		enums = items;

	}

	public String getValue() {
		return enums[Math.abs(rand.nextInt()) % enums.length];
	}

	@Override
	public String toString() {
		return "EnumarItem [enums=" + Arrays.toString(enums) + "]";
	}

}

class CharVarItem extends StringItem {

	List<char[]> rang = new ArrayList<char[]>();
	int minLen = 1;
	int maxLen = 256;

	public void initString(String content) {

		// char([a-z]1:3)
		int start = content.indexOf('[');
		int end = content.indexOf(']');
		String[] items = content.substring(start + 1, end).split(",");
		for (String itemStr : items) {
			if (itemStr.indexOf('-') > 0) {
				String[] pair = itemStr.split("-");
				char[] curRange = new char[2];
				curRange[0] = pair[0].charAt(0);
				curRange[1] = pair[1].charAt(0);
				rang.add(curRange);
			}
		}
		int splitPos = content.indexOf(':');
		if (splitPos > 0) {
			int splitStart = (end == -1) ? content.indexOf('(') : end;
			int splitEnd = content.indexOf(')');
			String[] pair = content.substring(splitStart + 1, splitEnd).split(
					":");
			minLen = Integer.valueOf(pair[0]);
			maxLen = Integer.valueOf(pair[1]);
		}

	}

	public String getValue() {
		int lenth = Math.abs(rand.nextInt()) % (maxLen - minLen + 1) + minLen;
		char[] chars = new char[lenth];
		for (int i = 0; i < chars.length; i++) {
			int randInt = Math.abs(rand.nextInt());
			int choise = randInt % rang.size();
			char[] choisedRange = rang.get(choise);
			char randChar = (char) (randInt
					% (choisedRange[1] - choisedRange[0] + 1) + choisedRange[0]);
			chars[i] = randChar;

		}
		return new String(chars);
	}

	@Override
	public String toString() {
		return "CharVarItem [rang=" + rang + ", minLen=" + minLen + ", maxLen="
				+ maxLen + "]";
	}

}

class StringItem {
	protected static final Random rand = new Random();
	public String content;

	public StringItem() {

	}

	public String getValue() {
		return content;
	}

	public void initString(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "StringItem [content=" + content + "]";
	}
}

class IntVarItem extends StringItem {
	long[] rang = { 0, Integer.MAX_VALUE };
	long[] enums = {};
	boolean isEnumInt = false;

	public void initString(String content) {
		int start = content.indexOf('(');
		int end = content.indexOf(')');
		if (content.indexOf('-') > 0) {
			String range = content.substring(start + 1, end);
			String[] items = range.split("-");
			rang[0] = Integer.valueOf(items[0]);
			rang[1] = Integer.valueOf(items[1]);
		} else {
			isEnumInt = true;
			String range = content.substring(start + 1, end);
			String[] items = range.split(",");
			enums = new long[items.length];
			for (int i = 0; i < enums.length; i++) {
				enums[i] = Long.valueOf(items[i]);
			}
		}

	}

	public String getValue() {
		if (isEnumInt) {
			return enums[Math.abs(rand.nextInt()) % enums.length] + "";
		} else {
			long span = rang[1] - rang[0] + 1;
			return Math.abs(rand.nextInt()) % span + rang[0] + "";
		}
	}

	@Override
	public String toString() {
		return "IntVarItem [rang=" + Arrays.toString(rang) + ", enums="
				+ Arrays.toString(enums) + ", isEnumInt=" + isEnumInt + "]";
	}
}

class DateVarItem extends StringItem {
	String format;
	int[] yearRang = { 1970, 2999 };
	int[] monRang = { 1, 12 };
	int[] dayRang = { 1, 31 };
	int[] hourRang = { 0, 23 };
	int[] minuteRang = { 0, 59 };
	int[] secondRang = { 0, 59 };
	int[] sssRang = { 0, 999 };

	public DateVarItem() {

	}

	public void initString(String content) {
		int fmtEndPos = content.indexOf('-');
		if (fmtEndPos == -1) {
			fmtEndPos = content.indexOf(')');
		}
		format = content.substring(5, fmtEndPos);
		int yearP = content.indexOf("]y", fmtEndPos);
		if (yearP > 0) {
			yearRang = getRangeofPattern(content, yearP);
		}

		int monthP = content.indexOf("]M", fmtEndPos);
		if (monthP > 0) {
			monRang = getRangeofPattern(content, monthP);
		}

		int dayP = content.indexOf("]d", fmtEndPos);
		if (dayP > 0) {
			dayRang = getRangeofPattern(content, dayP);
		}
		int hourP = content.indexOf("]H", fmtEndPos);
		if (hourP > 0) {
			hourRang = getRangeofPattern(content, hourP);
		}
		int minuteP = content.indexOf("]m", fmtEndPos);
		if (minuteP > 0) {
			minuteRang = getRangeofPattern(content, minuteP);
		}
		int secondP = content.indexOf("]s", fmtEndPos);
		if (secondP > 0) {
			secondRang = getRangeofPattern(content, secondP);
		}
		int millisS = content.indexOf("]S", fmtEndPos);
		if (millisS > 0) {
			sssRang = getRangeofPattern(content, millisS);
		}
	}

	private static final int[] getRangeofPattern(String theString, int endPos) {
		String subString = theString.substring(0, endPos);
		int start = subString.lastIndexOf('[');
		String range = subString.substring(start + 1, endPos);
		String[] items = range.split("-");
		int[] values = new int[2];
		values[0] = Integer.valueOf(items[0]);
		values[1] = Integer.valueOf(items[1]);
		return values;
	}

	public String getValue() {
		int yearSpan = yearRang[1] - yearRang[0] + 1;
		int year = Math.abs(rand.nextInt()) % yearSpan + yearRang[0];

		int monthSpan = monRang[1] - monRang[0] + 1;
		int month = Math.abs(rand.nextInt()) % monthSpan + monRang[0];

		int daySpan = dayRang[1] - dayRang[0] + 1;
		int day = Math.abs(rand.nextInt()) % daySpan + dayRang[0];

		int hourSpan = hourRang[1] - hourRang[0] + 1;
		int hour = Math.abs(rand.nextInt()) % hourSpan + hourRang[0];

		int minuteSpan = minuteRang[1] - minuteRang[0] + 1;
		int minute = Math.abs(rand.nextInt()) % minuteSpan + minuteRang[0];

		int secondSpan = secondRang[1] - secondRang[0] + 1;
		int second = Math.abs(rand.nextInt()) % secondSpan + secondRang[0];

		int sssSpan = sssRang[1] - sssRang[0] + 1;
		int sss = Math.abs(rand.nextInt()) % sssSpan + sssRang[0];

		java.util.Calendar cl = Calendar.getInstance();
		cl.set(Calendar.YEAR, year);
		cl.set(Calendar.MONTH, month);
		cl.set(Calendar.DATE, day);
		cl.set(Calendar.HOUR_OF_DAY, hour);
		cl.set(Calendar.MINUTE, minute);
		cl.set(Calendar.SECOND, second);
		cl.set(Calendar.MILLISECOND, sss);
		return new java.text.SimpleDateFormat(format).format(cl.getTime());

	}

	@Override
	public String toString() {
		return "DateVarItem [format=" + format + ", yearRang="
				+ Arrays.toString(yearRang) + ", monRang="
				+ Arrays.toString(monRang) + ", dayRang="
				+ Arrays.toString(dayRang) + "]";
	}

}
