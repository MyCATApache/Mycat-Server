package io.mycat.parser.druid;

import static junit.framework.Assert.assertEquals;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import io.mycat.config.model.SystemConfig;
import io.mycat.route.parser.druid.DruidSequenceHandler;

/**
 * 获取MYCAT SEQ 表名。
 */
public class DruidSequenceHandlerTest {

	@Test
	public void test() {
		DruidSequenceHandler handler = new DruidSequenceHandler(SystemConfig.SEQUENCEHANDLER_LOCALFILE);
		
		String sql = "select next value for mycatseq_xxxx".toUpperCase();
		String tableName = handler.getTableName(sql);
		assertEquals(tableName, "XXXX");


        sql = " insert into test(id,sid)values(next value for MYCATSEQ_TEST,1)".toUpperCase();
         tableName = handler.getTableName(sql);
        assertEquals(tableName, "TEST");

        sql = " insert into test(id,sid)values(next value for MYCATSEQ_TEST       ,1)".toUpperCase();
        tableName = handler.getTableName(sql);
        assertEquals(tableName, "TEST");

        sql = " insert into test(id)values(next value for MYCATSEQ_TEST  )".toUpperCase();
        tableName = handler.getTableName(sql);
        assertEquals(tableName, "TEST");
	}

	@Test
	public void test2() {
		DruidSequenceHandler handler = new DruidSequenceHandler(SystemConfig.SEQUENCEHANDLER_LOCALFILE);
		
		String sql = "/* APPLICATIONNAME=DBEAVER 3.3.2 - MAIN CONNECTION */ SELECT NEXT VALUE FOR MYCATSEQ_XXXX".toUpperCase();
		String tableName = handler.getTableName(sql);
		assertEquals(tableName, "XXXX");
	}

    public static void main(String[] args)
    {
        String patten="(?:(\\s*next\\s+value\\s+for\\s*MYCATSEQ_(\\w+))(,|\\)|\\s)*)+";
        Pattern pattern = Pattern.compile(patten,Pattern.CASE_INSENSITIVE);
         String sql="insert into test(id,sid)values(    next    value    for        MYCATSEQ_TEST ,1)";
          Matcher matcher = pattern.matcher(sql);
        System.out.println(matcher.find());
        System.out.println(matcher.group(1));
        System.out.println(matcher.group(2));
    }
}
