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

/**
 * @author songwie
 */
public final class ManagerParseHeartbeat {

    public static final int OTHER = -1;
    public static final int DATASOURCE = 1;
    
   // SHOW @@HEARTBEAT
    static int show2HeaCheck(String stmt, int offset) {
        if (stmt.length() > offset + "RTBEAT".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'T' || c2 == 't') & (c3 == 'B' || c3 == 'b')
                    && (c4 == 'E' || c4 == 'e') & (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't')) {
            	if (stmt.length() > offset + ".DETAIL".length()) {
            		char c7 = stmt.charAt(++offset);
                	if(c7 == '.'){
                		return show2HeaDetailCheck(stmt,offset);
                	}
            	}
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return ManagerParseShow.HEARTBEAT;
            }
        }
        return OTHER;
    }
    // SHOW @@HEARTBEAT.DETAIL
    static int show2HeaDetailCheck(String stmt, int offset) {
        if (stmt.length() > offset + "DETAIL".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && (c2 == 'E' || c2 == 'e') & (c3 == 'T' || c3 == 't')
                    && (c4 == 'A' || c4 == 'a') & (c5 == 'I' || c5 == 'i') && (c6 == 'L' || c6 == 'l')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return ManagerParseShow.HEARTBEAT_DETAIL;
            }
        }
        return OTHER;
    }

    public static Pair<String, String> getPair(String stmt) {
        int offset = stmt.indexOf("@@");
        String s = stmt.substring(++offset + " heartbeat.detail".length());
        char c = s.charAt(0);
        offset = 0;
        if(c == ' '){
        	char c1 = s.charAt(++offset);
    		char c2 = s.charAt(++offset);
    		char c3 = s.charAt(++offset);
    		char c4 = s.charAt(++offset);
    		char c5 = s.charAt(++offset);
    		char c6 = s.charAt(++offset);
    		char c7 = s.charAt(++offset);
    		char c8 = s.charAt(++offset);
    		char c9 = s.charAt(++offset);
    		char c10 = s.charAt(++offset);
    		char c11 = s.charAt(++offset);
    		if ((c1 == 'W' || c1 == 'w') && (c2 == 'H' || c2 == 'h') && (c3 == 'E' || c3 == 'e')
                    && (c4 == 'R' || c4 == 'r') && (c5 == 'E' || c5 == 'e')
                    && c6 == ' ' && (c7 == 'N' || c7 == 'n') && (c8 == 'A' || c8 == 'a') && (c9 == 'M' || c9 == 'm')
                    && (c10 == 'E' || c10 == 'e') && (c11 == '=')) {
    	        String name = s.substring(++offset).trim();
                return new Pair<String, String>("name", name);
    		}
        }
        return new Pair<String, String>("name", "");
    }
 
}