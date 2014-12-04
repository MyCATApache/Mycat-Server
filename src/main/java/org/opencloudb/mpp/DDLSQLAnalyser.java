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
package org.opencloudb.mpp;

import java.sql.SQLSyntaxErrorException;

import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.CreateIndexNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.DropIndexNode;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.QueryTreeNode;

/**
 * DDL sql analyser
 * 
 * @author wuzhih
 * 
 */

public class DDLSQLAnalyser {

	public static DDLParsInf analyse(QueryTreeNode ast)
			throws SQLSyntaxErrorException {
		DDLParsInf parsInf=new DDLParsInf();
		DDLStatementNode ddlNode = (DDLStatementNode) ast;
		String tableName=null;
		if(ddlNode instanceof CreateTableNode)
		{
			tableName=((CreateTableNode)ddlNode).getObjectName().getTableName();
		}else if(ddlNode instanceof AlterTableNode)
		{
			tableName=((AlterTableNode)ddlNode).getObjectName().getTableName();
		}else if(ddlNode instanceof DropTableNode)
		{
			tableName=((DropTableNode)ddlNode).getObjectName().getTableName();
		}else if(ddlNode instanceof CreateIndexNode)
		{
			tableName=((CreateIndexNode)ddlNode).getObjectName().getTableName();
		}else if(ddlNode instanceof DropIndexNode)
		{
			tableName=((DropIndexNode)ddlNode).getObjectName().getTableName();
		}else
		{
			throw new SQLSyntaxErrorException("stmt not supported yet: "+ddlNode.getClass().getSimpleName());
		}
		parsInf.tableName=tableName.toUpperCase();
		return parsInf;
	}
}