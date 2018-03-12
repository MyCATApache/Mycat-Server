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

import io.mycat.route.parser.util.Pair;
import io.mycat.route.parser.util.ParseUtil;
import io.mycat.util.SplitUtil;

/**
 * @author mycat
 */
public final class ManagerParseStop {

    public static final int OTHER = -1;
    public static final int HEARTBEAT = 1;

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
                return stop2Check(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    public static Pair<String[], Integer> getPair(String stmt) {
        int offset = stmt.indexOf("@@");
        String s = stmt.substring(offset + 11).trim();
        int p1 = s.lastIndexOf(':');
        if (p1 == -1) {
            String[] src = SplitUtil.split(s, ',', '$', '-', '[', ']');
            return new Pair<String[], Integer>(src, null);
        } else {
            String[] src = SplitUtil.split(s, ':', true);
            String[] src1 = SplitUtil.split(src[0], ',', '$', '-', '[', ']');
            return new Pair<String[], Integer>(src1, Integer.valueOf(src[1]));
        }
    }

    // HEARTBEAT
    static int stop2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@'
                && stmt.length() > offset + 9) {
                char c1 = stmt.charAt(++offset);
                char c2 = stmt.charAt(++offset);
                char c3 = stmt.charAt(++offset);
                char c4 = stmt.charAt(++offset);
                char c5 = stmt.charAt(++offset);
                char c6 = stmt.charAt(++offset);
                char c7 = stmt.charAt(++offset);
                char c8 = stmt.charAt(++offset);
                char c9 = stmt.charAt(++offset);
                if ((c1 == 'H' || c1 == 'h') && (c2 == 'E' || c2 == 'e') && (c3 == 'A' || c3 == 'a')
                        && (c4 == 'R' || c4 == 'r') && (c5 == 'T' || c5 == 't') && (c6 == 'B' || c6 == 'b')
                        && (c7 == 'E' || c7 == 'e') && (c8 == 'A' || c8 == 'a') && (c9 == 'T' || c9 == 't')) {
                    if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                        return OTHER;
                    }
                    return HEARTBEAT;
                }
        }
        return OTHER;
    }

}