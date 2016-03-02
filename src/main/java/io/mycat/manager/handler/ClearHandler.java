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
package io.mycat.manager.handler;

import io.mycat.config.ErrorCode;
import io.mycat.manager.ManagerConnection;
import io.mycat.manager.response.ClearSlow;
import io.mycat.route.parser.ManagerParseClear;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public class ClearHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseClear.parse(stmt, offset);
        switch (rs & 0xff) {
        case ManagerParseClear.SLOW_DATANODE: {
            String name = stmt.substring(rs >>> 8).trim();
            if (StringUtil.isEmpty(name)) {
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            } else {
                ClearSlow.dataNode(c, name);
            }
            break;
        }
        case ManagerParseClear.SLOW_SCHEMA: {
            String name = stmt.substring(rs >>> 8).trim();
            if (StringUtil.isEmpty(name)) {
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            } else {
                ClearSlow.schema(c, name);
            }
            break;
        }
        default:
            c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}