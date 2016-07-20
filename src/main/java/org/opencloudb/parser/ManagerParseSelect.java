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

import org.opencloudb.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseSelect {

    public static final int OTHER = -1;
    public static final int VERSION_COMMENT = 1;
    public static final int SESSION_AUTO_INCREMENT = 2;
    public static final int SESSION_TX_READ_ONLY = 3;

    private static final char[] _VERSION_COMMENT = "VERSION_COMMENT".toCharArray();
    private static final char[] _SESSION_AUTO_INCREMENT = "SESSION.AUTO_INCREMENT_INCREMENT".toCharArray();
    private static final char[] _SESSION_TX_READ_ONLY = "SESSION.TX_READ_ONLY".toCharArray();

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
            case ' ':
                continue;
            case '/':
            case '#':
                i = ParseUtil.comment(stmt, i);
                continue;
            case '@':
                return select2Check(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    static int select2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
            if (stmt.length() > ++offset) {
                switch (stmt.charAt(offset)) {
                case 'S':
                case 's':
                    return select2SCheck(stmt, offset);
                case 'V':
                case 'v':
                    return select2VCheck(stmt, offset);
                default:
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // VERSION_COMMENT
    static int select2VCheck(String stmt, int offset) {
        int length = offset + _VERSION_COMMENT.length;
        if (stmt.length() >= length) {
            if (ParseUtil.compare(stmt, offset, _VERSION_COMMENT)) {
                if (stmt.length() > length && stmt.charAt(length) != ' ') {
                    return OTHER;
                }
                return VERSION_COMMENT;
            }
        }
        return OTHER;
    }

    // SESSION.AUTO_INCREMENT_INCREMENT  or SESSION.TX_READ_ONLY
    static int select2SCheck(String stmt, int offset) {
        int length = offset + _SESSION_AUTO_INCREMENT.length;
        int length_tx_read_only = offset + _SESSION_TX_READ_ONLY.length;
        if ((stmt.length() >= length)
                && (ParseUtil.compare(stmt, offset, _SESSION_AUTO_INCREMENT))) {
                if (stmt.length() > length && stmt.charAt(length) != ' ') {
                    return OTHER;
                }
                return SESSION_AUTO_INCREMENT;
        } else if ((stmt.length() >= length_tx_read_only)
                && ParseUtil.compare(stmt, offset, _SESSION_TX_READ_ONLY)) {
            if (stmt.length() > length_tx_read_only && stmt.charAt(length_tx_read_only) != ' ') {
                return OTHER;
            }
            return SESSION_TX_READ_ONLY;
        }

        return OTHER;
    }

}