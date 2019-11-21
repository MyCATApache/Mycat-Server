package io.mycat.util;
import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import io.mycat.server.util.SchemaUtil;
/**
 * @author stones-he
 */
public class SchemaUtilTest {
    @Test
    public void parseShowTableTest() {
        String stmt = "SHOW FULL TABLES FROM schema001 WHERE Tables_in_schema001 LIKE 'tab_company'; ";
        String[] fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1",
                                       "FULL",
                                       "FROM",
                                       "schema001",
                                       "WHERE",
                                       "Tables_in_schema001",
                                       "LIKE",
                                       "'tab_company'",
                                       "tab_company"}, fields);
    }
    @Test
    public void parseShowTableTest1() {
        String stmt = "SHOW FULL TABLES LIKE 'tab_company' ";
        String[] fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", "FULL", null, null, null, null, "LIKE", "'tab_company'", "tab_company"},
                          fields);
    }
    @Test
    public void parseShowTableTest2() {
        String stmt = "SHOW FULL TABLES IN mysql LIKE 'time%'; ";
        String[] fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", "FULL", "IN", "mysql", null, null, "LIKE", "'time%'", "time%"}, fields);
        stmt = "SHOW FULL TABLES IN mysql LIKE '%time%'; ";
        fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", "FULL", "IN", "mysql", null, null, "LIKE", "'%time%'", "%time%"}, fields);
        stmt = "SHOW FULL TABLES IN mysql LIKE '%time'; ";
        fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", "FULL", "IN", "mysql", null, null, "LIKE", "'%time'", "%time"}, fields);
    }
    @Test
    public void parseShowTableTest3() {
        String stmt = "SHOW TABLES WHERE table_type = 'BASE TABLE';";
        String[] fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", null, null, null, "WHERE", "table_type", "=", "'BASE TABLE'", "BASE TABLE"}, fields);
    }
    @Test
    public void parseShowTableTest4() {
        String stmt = "SHOW TABLES WHERE table_type = 'BASE TABLE';";
        String[] fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", null, null, null, "WHERE", "table_type", "=", "'BASE TABLE'", "BASE TABLE"}, fields);
        //
        stmt = "SHOW FULL TABLES WHERE table_type = 'BASE TABLE';";
        fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", "FULL", null, null, "WHERE", "table_type", "=", "'BASE TABLE'", "BASE TABLE"}, fields);
        //
        stmt = "SHOW FULL TABLES in schema001 WHERE table_type = 'BASE TABLE';";
        fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", "FULL", "in", "schema001", "WHERE", "table_type", "=", "'BASE TABLE'", "BASE TABLE"}, fields);
        //
        stmt = "SHOW FULL TABLES WHERE table_type LIKE '%BASE TABLE';";
        fields = SchemaUtil.parseShowTable(stmt);
        assertArrayEquals(new String[]{"1", "FULL", null, null, "WHERE", "table_type", "LIKE", "'%BASE TABLE'", "%BASE TABLE"}, fields);
         //
         stmt = "SHOW FULL TABLES WHERE tables_in_0001 LIKE '%BASE TABLE';";
         fields = SchemaUtil.parseShowTable(stmt);
         assertArrayEquals(new String[]{"1", "FULL", null, null, "WHERE", "tables_in_0001", "LIKE", "'%BASE TABLE'", "%BASE TABLE"}, fields);
         
    }
    //SHOW FULL TABLES in miccore WHERE table_type = 'BASE TABLE';
}