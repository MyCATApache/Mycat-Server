package org.opencloudb.parser.druid;

import static junit.framework.Assert.*;

import org.junit.Test;
import org.opencloudb.config.model.SystemConfig;

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
	}

	@Test
	public void test2() {
		DruidSequenceHandler handler = new DruidSequenceHandler(SystemConfig.SEQUENCEHANDLER_LOCALFILE);
		
		String sql = "/* APPLICATIONNAME=DBEAVER 3.3.2 - MAIN CONNECTION */ SELECT NEXT VALUE FOR MYCATSEQ_XXXX".toUpperCase();
		String tableName = handler.getTableName(sql);
		assertEquals(tableName, "XXXX");
	}
}
