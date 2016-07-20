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
package org.opencloudb.handler;

import static org.opencloudb.parser.ManagerParseSelect.SESSION_AUTO_INCREMENT;
import static org.opencloudb.parser.ManagerParseSelect.VERSION_COMMENT;
import static org.opencloudb.parser.ManagerParseSelect.SESSION_TX_READ_ONLY;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.parser.ManagerParseSelect;
import org.opencloudb.response.SelectSessionAutoIncrement;
import org.opencloudb.response.SelectTxReadOnly;
import org.opencloudb.response.SelectVersionComment;

/**
 * @author mycat
 */
public final class SelectHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseSelect.parse(stmt, offset)) {
        case VERSION_COMMENT:
            SelectVersionComment.execute(c);
            break;
        case SESSION_AUTO_INCREMENT:
            SelectSessionAutoIncrement.execute(c);
            break;
        case SESSION_TX_READ_ONLY:
            SelectTxReadOnly.response(c);
            break;
        default:
            c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}