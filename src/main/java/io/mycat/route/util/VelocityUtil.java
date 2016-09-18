package io.mycat.route.util;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ParseErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * 
 * @author yan.yan@huawei.com
 *
 */
public class VelocityUtil {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(VelocityUtil.class);

	private static DateUtil dateUtil = new DateUtil();
	private static StringUtil stringUtil = new StringUtil();

	private static VelocityContext getContext() {
		VelocityContext context = new VelocityContext();
		context.put("dateUtil", dateUtil);
		context.put("stringUtil", stringUtil);
		return context;
	}

	/**
	 * 
	 * 
	 * @param tc
	 * @param colsVal
	 * @return
	 */
	public static String evalDBRule(String columnName, Object value,
			String dbRule) {

		String dbIndex = "";
		VelocityContext context = getContext();
		Writer writer = new StringWriter();
		try {
			context.put(columnName, value);
			Velocity.evaluate(context, writer, StringUtil.EMPTY, dbRule);
			dbIndex = StringUtil.trim(writer.toString());
			if (StringUtil.isEmpty(dbIndex)) {
				return "0";
			}
			return dbIndex;
		} catch (ParseErrorException e) {
			throw e;
		} catch (Exception e) {
			if (LOGGER.isDebugEnabled()) {
				// LOGGER.debug(tc.getName() + "eval " + dbRule + " error..");

			}
		}

		return "0";

	}

	public static void main(String[] args) throws IOException {
		String rule = "#set($Integer=0)##\r\n"
				+ "#set($db=$ID%10)##\r\n"
				+ "#set($tb=($ID%100)/10)##\r\n"
				+ "#set($prefix='0'+$Integer.toString($db)+'_'+$Integer.toString($tb))##\r\n"
				+ "$!prefix\r\n";

		String rule2 = "#set($Integer=0)##\r\n"
				+ "#set($monthstr=$stringUtil.substring($ID,4,6))##\r\n"
				+ "#set($month=$Integer.parseInt($monthstr))##\r\n"
				+ "#set($daystr=$stringUtil.substring($ID,6,8))##\r\n"
				+ "#set($day=$Integer.parseInt($daystr))##\r\n"
				+ "#if($month == 1)##\r\n" + "#set($n=0)##\r\n"
				+ "#elseif($month ==2)##\r\n" + "#set($n=31)##\r\n"
				+ "#elseif($month ==3)##\r\n" + "#set($n=31+28)##\r\n"
				+ "#elseif($month ==4)##\r\n" + "#set($n=31+28+31)##\r\n"
				+ "#elseif($month ==5)##\r\n" + "#set($n=31+28+31+30)##\r\n"
				+ "#elseif($month ==6)##\r\n" + "#set($n=31+28+31+30+31)##\r\n"
				+ "#elseif($month ==7)##\r\n"
				+ "#set($n=31+28+31+30+31+30)##\r\n"
				+ "#elseif($month ==8)##\r\n"
				+ "#set($n=31+28+31+30+31+30+31)##\r\n"
				+ "#elseif($month ==9)##\r\n"
				+ "#set($n=31+28+31+30+31+30+31+31)##\r\n"
				+ "#elseif($month ==10)##\r\n"
				+ "#set($n=31+28+31+30+31+30+31+31+30)##\r\n"
				+ "#elseif($month ==11)##\r\n"
				+ "#set($n=31+28+31+30+31+30+31+31+30+31)##\r\n" + "#else\r\n"
				+ "#set($n=31+28+31+30+31+30+31+31+30+31+30)##\r\n"
				+ "#end\r\n" + "#set($prefix=$n+$day+(-1))##\r\n" + "$!prefix";
		String rule3 = "#set($Integer=0)##\r\n"
				+ "#set($monthday=$stringUtil.substring($ID,2,8))##\r\n"
				+ "#set($prefix=$monthday.hashCode()%100)##\r\n" 
				+ "$!prefix";
		String ret = evalDBRule("ID", "201508202330011", rule3);
		System.out.println(ret);
		String tpl = " #set($db_flag=$!stringUtil.crc32($F_CERTIFICATE_CODE))\r\n"
				+ "$!stringUtil.substring($db_flag,-3,-1)";
		Writer writer = new StringWriter();
		try {
			VelocityContext context = getContext();
			context.put("F_CERTIFICATE_CODE", "123123123123123");
			Velocity.evaluate(context, writer, "", tpl);
			System.out.println(writer.toString());
		} catch (ParseErrorException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			writer.close();
		}

	}
}
