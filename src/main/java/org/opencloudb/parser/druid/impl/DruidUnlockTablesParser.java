/*
 * Copyright (c) 2015 xiaomi.com. All Rights Reserved.
 */
package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;

import java.sql.SQLNonTransientException;

/**
 * 支持 UNLOCK TABLES
 *
 * @author: dengliren Date: 2015-02-13 上午10:02
 * @version: \$Id$
 */
public class DruidUnlockTablesParser extends DefaultDruidParser {

    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        RouterUtil.routeToAllNodes(schema, rrs, ctx.getSql(), true);
        if (rrs.getNodes() != null && rrs.getNodes().length > 0) {
            rrs.setFinishedRoute(true);
        }
    }
}
