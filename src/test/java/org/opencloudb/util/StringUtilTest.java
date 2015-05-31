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
package org.opencloudb.util;

import java.io.PrintWriter;
import java.io.StringWriter;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author mycat
 */
public class StringUtilTest {

    @Test
    public void test() {
        String oriSql = "insert into ssd  (id) values (s)";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("ssd", tableName);
    }

    @Test
    public void test1() {
    	String oriSql = "insert into    ssd(id) values (s)";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("ssd", tableName);
    }

    @Test
    public void test2() {
    	String oriSql = "  insert  into    ssd(id) values (s)";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("ssd", tableName);
    }

    @Test
    public void test3() {
    	String oriSql = "  insert  into    isd(id) values (s)";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("isd", tableName);
    }

    @Test
    public void test4() {
    	String oriSql = "INSERT INTO test_activity_input  (id,vip_no";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("test_activity_input", tableName);
    }
    
    @Test
    public void test5() {
    	String oriSql = " /* ApplicationName=DBeaver 3.3.1 - Main connection */ insert into employee(id,name,sharding_id) values(4, 'myhome', 10011)";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    
    @Test
    public void test6() {
    	String oriSql = " /* insert int a (id, name) value(1, 'ben') */ insert into employee(id,name,sharding_id) values(4, 'myhome', 10011)";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    
    @Test
    public void test7() {
    	String oriSql = " /**/ insert into employee(id,name,sharding_id) values(4, 'myhome', 10011)";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    
    @Test
    public void test8() {
    	String oriSql = " /*  */ insert into employee(id,name,sharding_id) values(4, 'myhome', 10011) /**/";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    
    @Test
    public void test9() {
    	String oriSql = " /* hint1 insert */ /**/ /* hint3 insert */ insert into employee(id,name,sharding_id) values(4, 'myhome', 10011) /**/";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    
    @Test
    public void test10() {
    	String oriSql = " /* hint1 insert */ /* // */ /* hint3 insert */ insert into employee(id,name,sharding_id) values(4, 'myhome', 10011) /**/";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    
    @Test
    public void test11() {
    	String oriSql = " /* hint1 insert */ /* // */ /* hint3 insert */ insert /*  */ into employee(id,name,sharding_id) values(4, 'myhome', 10011) /**/";
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    @Test
    public void test12() {
    	StringWriter sw=new StringWriter();
    	PrintWriter pw=new PrintWriter(sw);
    	pw.println("insert into");
    	pw.println(" employee(id,name,sharding_id) values(4, 'myhome', 10011)");
    	pw.flush();
    	String oriSql = sw.toString();
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
    @Test
    public void test13() {
    	StringWriter sw=new StringWriter();
    	PrintWriter pw=new PrintWriter(sw);
    	pw.println("insert into");
    	pw.println("employee(id,name,sharding_id) values(4, 'myhome', 10011)");
    	pw.flush();
    	String oriSql = sw.toString();
        String tableName = StringUtil.getTableName(oriSql);
        Assert.assertEquals("employee", tableName);
    }
}