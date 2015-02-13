/*
 * Copyright (c) 2015 xiaomi.com. All Rights Reserved.
 */
package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;

import java.sql.SQLNonTransientException;

/**
 * LOCK TABLES 语句支持
 *
 * @author: dengliren Date: 2015-02-13 上午9:34
 * @version: \$Id$
 */
public class DruidLockTableParser extends DefaultDruidParser {

    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        MySqlLockTableStatement lockTable = (MySqlLockTableStatement) stmt;
        String tableName = removeBackquote(lockTable.getTableSource().toString().toUpperCase());
        ctx.addTable(tableName);
        TableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            throw new SQLNonTransientException("table '"+ tableName +"' not exists.");
        }
        RouterUtil.routeToMultiNode(true, rrs, tc.getDataNodes(), ctx.getSql());
        if (rrs.getNodes() != null && rrs.getNodes().length > 0) {
            rrs.setFinishedRoute(true);
        }
    }
}
