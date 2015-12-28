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
public final class ManagerParseReload {

    public static final int OTHER = -1;
    public static final int CONFIG = 1;
    public static final int ROUTE = 2;
    public static final int USER = 3;
    public static final int USER_STAT = 4;
    public static final int CONFIG_ALL = 5;
    public static final int SQL_SLOW = 6;
    public static final int SQL_STAT = 7;
    public static final int QUERY_CF = 8;
       
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
                return reload2Check(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    static int reload2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
            if (stmt.length() > ++offset) {
                switch (stmt.charAt(offset)) {
                case 'C':
                case 'c':
                    return reload2CCheck(stmt, offset);
                case 'R':
                case 'r':
                    return reload2RCheck(stmt, offset);
                case 'U':
                case 'u':
                    return reload2UCheck(stmt, offset);
                case 'S':
                case 's':
                    return reload2SCheck(stmt, offset);       
                case 'Q':
                case 'q':
                    return reload2QCheck(stmt, offset);     
                default:
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // RELOAD @@CONFIG
    static int reload2CCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'N' || c2 == 'n') && (c3 == 'F' || c3 == 'f')
                    && (c4 == 'I' || c4 == 'i') && (c5 == 'G' || c5 == 'g')) {
                if (stmt.length() > offset + 4)
                {
                    char c6 = stmt.charAt(++offset);
                    char c7 = stmt.charAt(++offset);
                    char c8 = stmt.charAt(++offset);
                    char c9 = stmt.charAt(++offset);
                    if ((c6 == '_' || c6 == '-') && (c7 == 'A' || c7 == 'a') && (c8 == 'L' || c8 == 'l')
                            && (c9 == 'L' || c9 == 'l') ) {
                          return CONFIG_ALL;
                    }
                }
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }

                return CONFIG;
            }
        }
        return OTHER;
    }

    // RELOAD @@ROUTE
    static int reload2RCheck(String stmt, int offset) {
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

    // RELOAD @@USER
    static int reload2UCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r')) {
            	
            	
            	if (stmt.length() > offset + 5)
                {
                    char c6 = stmt.charAt(++offset);
                    char c7 = stmt.charAt(++offset);
                    char c8 = stmt.charAt(++offset);
                    char c9 = stmt.charAt(++offset);
                    char c10 = stmt.charAt(++offset);
                    
                    if ((c6 == '_' || c6 == '-') && (c7 == 'S' || c7 == 's') && (c8 == 'T' || c8 == 't')
                            && (c9 == 'A' || c9 == 'a') && (c10 == 'T' || c10 == 't') ) {
                          return USER_STAT;
                    }
                }
            	
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return USER;
            }
        }
        return OTHER;
    }
    
    // RELOAD @@SQL
    static int reload2SCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);  
            
            // reload @@sqlslow
            if ((c1 == 'Q' || c1 == 'q') && (c2 == 'L' || c2 == 'l') && (c3 == 's' || c3 == 'S')
                    && (c4 == 'L' || c4 == 'l') && (c5 == 'O' || c5 == 'o') && (c6 == 'W' || c6 == 'w') ) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return SQL_SLOW ;
                }
            }
            
            // reload @@sqlstat
            if ((c1 == 'Q' || c1 == 'q') && (c2 == 'L' || c2 == 'l') && (c3 == 's' || c3 == 'S')
                    && (c4 == 'T' || c4 == 't') && (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't') ) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return SQL_STAT ;
                }
            }
            
            return OTHER;
        }
        return OTHER;
    }
    
    // RELOAD @@QUERY
    static int reload2QCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);  
            char c7 = stmt.charAt(++offset);

            if ((c1 == 'U' || c1 == 'u') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r')
                    && (c4 == 'Y' || c4 == 'y') && (c5 == '_' ) && (c6 == 'C' || c6 == 'c') && (c7 == 'F' || c7 == 'f') ) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return QUERY_CF ;
                }
                return OTHER;
            }
        }
        return OTHER;
    }
}