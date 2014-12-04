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
package org.opencloudb.parser;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.opencloudb.config.model.SystemConfig;

import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.unparser.NodeToString;

public class SequenceParserTest {

	@Before
	public void initSeq() throws Exception {

		String filePath = Thread.currentThread().getContextClassLoader()
				.getResource("").getPath()
				+ "sequence_conf.properties";

		Properties props = new Properties();
		props.load(new FileInputStream(filePath));
		props.setProperty("TEST.HISIDS", "");
		props.setProperty("TEST.MINID", "101");
		props.setProperty("TEST.MAXID", "200");
		props.setProperty("TEST.CURID", "110");
		OutputStream fos = new FileOutputStream(filePath);
		props.store(fos, "");
		fos.close();
	}

	@Test
	public void testParseSequence() throws Exception {

		ExtNodeToString4SEQ strHandler = new ExtNodeToString4SEQ(SystemConfig.SEQUENCEHANDLER_LOCALFILE);
		QueryTreeNode ast = null;
		String sqlText = "SELECT NEXT VALUE FOR MYCATSEQ_TEST from sys.tb1";
		ast = SQLParserDelegate.parse(sqlText,
				SQLParserDelegate.DEFAULT_CHARSET);
		Assert.assertEquals("SELECT 111 FROM sys.tb1", strHandler.toString(ast));

		sqlText = "SELECT NEXT VALUE FOR MyCatSEQ_TEST from sys.tb2";
		ast = SQLParserDelegate.parse(sqlText,
				SQLParserDelegate.DEFAULT_CHARSET);
		Assert.assertEquals("SELECT 112 FROM sys.tb2", strHandler.toString(ast));

		sqlText = "select * from sys.systables where ( next value for MYCATSEQ_test ) > col_a";
		ast = SQLParserDelegate.parse(sqlText,
				SQLParserDelegate.DEFAULT_CHARSET);
		Assert.assertEquals("SELECT * FROM sys.systables WHERE 113 > col_a",
				strHandler.toString(ast));

		sqlText = "select * from sys.systables where ( next value for seq_a ) > col_b";
		ast = SQLParserDelegate.parse(sqlText,
				SQLParserDelegate.DEFAULT_CHARSET);
		Assert.assertEquals(
				"SELECT * FROM sys.systables WHERE (NEXT VALUE FOR seq_a) > col_b",
				strHandler.toString(ast));

		sqlText = "insert into tb3( a,b ) values ( next value for mycatseq_test,'micmiu' )";
		ast = SQLParserDelegate.parse(sqlText,
				SQLParserDelegate.DEFAULT_CHARSET);
		Assert.assertEquals("INSERT INTO tb3(a, b) VALUES(114, 'micmiu')",
				strHandler.toString(ast));

		sqlText = "insert into tb4( a,b ) values ( next value for seq_b,'micmiu' )";
		ast = SQLParserDelegate.parse(sqlText,
				SQLParserDelegate.DEFAULT_CHARSET);
		Assert.assertEquals(
				"INSERT INTO tb4(a, b) VALUES((NEXT VALUE FOR seq_b), 'micmiu')",
				strHandler.toString(ast));

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String[] sqlTexts = new String[] {
				"SELECT NEXT VALUE FOR MYCATSEQ_TEST from sys.tb1",
				"SELECT NEXT VALUE FOR MyCatSEQ_TEST from sys.tb2",
				"select * from sys.systables where ( next value for MYCATSEQ_test ) > col_a",
				"select * from sys.systables where ( next value for seq_a ) > col_b",
				"insert into tb3( a,b ) values ( next value for mycatseq_test,'test' )",
				"insert into tb4( a,b ) values ( next value for seq_b,'test2' )" };

		NodeToString strHandler = new NodeToString();
		QueryTreeNode ast = null;
		for (String sqlText : sqlTexts) {
			System.out.println(">>>>>>>>>>>>>>>>>>");
			ast = SQLParserDelegate.parse(sqlText,
					SQLParserDelegate.DEFAULT_CHARSET);
			System.out.println("SQL =: \t" + strHandler.toString(ast));
		}

	}

}