package org.opencloudb.route;

import org.opencloudb.route.util.RouterUtil;

/**
 * Created by StoneGod on 2015/9/22.
 */



public class TestDDLTableParser {
    public static void main(String[] args) {
//        String sql = "insert into employee(id,name,sharding_id) values(5, 'wdw',10010)";
//        int count = 1000000;
//        long start = System.currentTimeMillis();
//        long end = System.currentTimeMillis();
////        for (int i = 0; i < count; i++) {
////            MySqlStatementParser parser = new MySqlStatementParser(sql);
////            SQLStatement statement = parser.parseStatement();
////        }
//        end = System.currentTimeMillis();
//        System.out.println(count + " times parse ,druid cost:" + (end - start) + "ms");
//
//
//        int ind = 0 ;
//        String token1 = "CREATE ";
//        String token2 = " TABLE ";
//        int createInd = "CREATE TABLE TEST1(ID INT)".indexOf(token1, ind);
//        System.out.print(createInd);
//        int tabInd = "CREATE TABLE TEST1(ID INT)".indexOf(token2, ind);
//        System.out.print(tabInd);
//        String ddl = "CREATE TABLE TEST1(ID INT)".substring(tabInd);
//        System.out.print(ddl);
        // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前


        String str = " create   table t1(id int)";
        str = str.trim().toUpperCase();
        String str1 = " CREATE   TABLE `t1`(id int)";
        str1 = str1.trim().toUpperCase().replace("`","");
        System.out.println("str:" + RouterUtil.getTableName(str, RouterUtil.getCreateTablePos(str, 0)));

        System.out.println("str1:" + RouterUtil.getTableName(str1, RouterUtil.getCreateTablePos(str1, 0)));

        String str2 = " Alter TABLE `t1`";
        str2 = str2.trim().toUpperCase().replace("`","");
        System.out.println("str2:" + RouterUtil.getTableName(str2, RouterUtil.getAlterTablePos(str2, 0)));

        String str3 = " Drop TABLE test.`t1`       ;";
        if (str3.endsWith(";"))
            str3 = str3.substring(0,str3.length()-2);
        str3 = str3.trim().toUpperCase().replace("`","");


        System.out.println("str3:" + RouterUtil.getTableName(str3, RouterUtil.getDropTablePos(str3, 0)));


        String str4 = " truncaTe TABLE test.`t1`       ;";
        if (str4.endsWith(";"))
            str4 = str4.substring(0,str4.length()-2);
        str4 = str4.trim().toUpperCase().replace("`","");


        System.out.println("str411:" + RouterUtil.getTableName(str4, RouterUtil.getTruncateTablePos(str4, 0)));


    }


}