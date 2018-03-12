/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights
 * reserved. DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER. This
 * code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation. This code is distributed in the hope that it will
 * be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License version 2 for more details (a copy is included in the LICENSE
 * file that accompanied this code). You should have received a copy of the GNU
 * General Public License version 2 along with this work; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA. Any questions about this component can be directed to it's
 * project Web address https://code.google.com/p/opencloudb/.
 */
package io.mycat.parser;

import static org.junit.Assert.assertEquals;
import io.mycat.MycatServer;
import io.mycat.server.interceptor.impl.DefaultSqlInterceptor;

import org.junit.Test;

public class TestEscapeProcess {

	String sql = "insert  into t_uud_user_account(USER_ID,USER_NAME,PASSWORD,CREATE_TIME,STATUS,NICK_NAME,USER_ICON_URL,USER_ICON_URL2,USER_ICON_URL3,ACCOUNT_TYPE) "
			+ "values (2488899998,'u\\'aa\\'\\'a''aa','af8f9dffa5d420fbc249141645b962ee','2013-12-01 00:00:00',0,NULL,NULL,NULL,NULL,1)";

	String sqlret = "insert  into t_uud_user_account(USER_ID,USER_NAME,PASSWORD,CREATE_TIME,STATUS,NICK_NAME,USER_ICON_URL,USER_ICON_URL2,USER_ICON_URL3,ACCOUNT_TYPE) "
			+ "values (2488899998,'u''aa''''a''aa','af8f9dffa5d420fbc249141645b962ee','2013-12-01 00:00:00',0,NULL,NULL,NULL,NULL,1)";

	String starWithEscapeSql = "\\insert  into t_uud_user_account(USER_ID,USER_NAME,PASSWORD,CREATE_TIME,STATUS,NICK_NAME,USER_ICON_URL,USER_ICON_URL2,USER_ICON_URL3,ACCOUNT_TYPE) "
			+ "values (2488899998,'u\\'aa\\'\\'a''aa','af8f9dffa5d420fbc249141645b962ee','2013-12-01 00:00:00',0,NULL,NULL,NULL,NULL,1)\\";

	String starWithEscapeSqlret = "\\insert  into t_uud_user_account(USER_ID,USER_NAME,PASSWORD,CREATE_TIME,STATUS,NICK_NAME,USER_ICON_URL,USER_ICON_URL2,USER_ICON_URL3,ACCOUNT_TYPE) "
			+ "values (2488899998,'u''aa''''a''aa','af8f9dffa5d420fbc249141645b962ee','2013-12-01 00:00:00',0,NULL,NULL,NULL,NULL,1)\\";

	
	@Test
	public void testEscapeProcess() {
		MycatServer.getInstance().getConfig().getSystem().setDefaultSqlParser("fdbparser");
		String sqlProcessed = DefaultSqlInterceptor.processEscape(sql);
		assertEquals(sqlProcessed, sqlret);
		String sqlProcessed1 = DefaultSqlInterceptor
				.processEscape(starWithEscapeSql);
		assertEquals(sqlProcessed1, starWithEscapeSqlret);
	}

}