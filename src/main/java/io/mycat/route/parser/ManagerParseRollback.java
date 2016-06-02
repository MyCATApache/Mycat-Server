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
package io.mycat.route.parser;

import io.mycat.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseRollback {

    public static final int OTHER = -1;
    public static final int CONFIG = 1;
    public static final int ROUTE = 2;
    public static final int USER = 3;

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
                return rollback2Check(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    static int rollback2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@'
                && stmt.length() > ++offset) {
                switch (stmt.charAt(offset)) {
                case 'C':
                case 'c':
                    return rollback2CCheck(stmt, offset);
                case 'R':
                case 'r':
                    return rollback2RCheck(stmt, offset);
                case 'U':
                case 'u':
                    return rollback2UCheck(stmt, offset);
                default:
                    return OTHER;
                }
        }
        return OTHER;
    }

    // ROLLBACK @@CONFIG
    static int rollback2CCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'N' || c2 == 'n') && (c3 == 'F' || c3 == 'f')
                    && (c4 == 'I' || c4 == 'i') && (c5 == 'G' || c5 == 'g')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return CONFIG;
            }
        }
        return OTHER;
    }

    // ROLLBACK @@ROUTE
    static int rollback2RCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'U' || c2 == 'u') && (c3 == 'T' || c3 == 't')
                    && (c4 == 'E' || c4 == 'e')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return ROUTE;
            }
        }
        return OTHER;
    }

    // ROLLBACK @@USER
    static int rollback2UCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return USER;
            }
        }
        return OTHER;
    }

}