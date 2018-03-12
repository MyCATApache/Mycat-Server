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
package io.mycat.route.parser.util;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.route.parser.druid.MycatStatementParser;

/**
 * @author mycat
 */
public final class ParseUtil {

    public static boolean isEOF(char c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';');
    }

    public static String parseString(String stmt) {
    	 int offset = stmt.indexOf('=');
         if (offset != -1 && stmt.length() > ++offset) {
             String txt = stmt.substring(offset).trim();
             return txt;
         }
         return null;
    }
    
    public static long getSQLId(String stmt) {
        int offset = stmt.indexOf('=');
        if (offset != -1 && stmt.length() > ++offset) {
            String id = stmt.substring(offset).trim();
            try {
                return Long.parseLong(id);
            } catch (NumberFormatException e) {
            }
        }
        return 0L;
    }

    public static String changeInsertAddSlot(String sql,int slotValue)
    {
        SQLStatementParser parser = new MycatStatementParser(sql);
        MySqlInsertStatement insert = (MySqlInsertStatement) parser.parseStatement();
        insert.getColumns().add(new SQLIdentifierExpr("_slot") );
        insert.getValues().getValues().add(new SQLIntegerExpr(slotValue))  ;
        return insert.toString();
    }
    /**
     * <code>'abc'</code>
     * 
     * @param offset stmt.charAt(offset) == first <code>'</code>
     */
    private static String parseString(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop: for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                case '0':
                    sb.append('\0');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'Z':
                    sb.append((char) 26);
                    break;
                default:
                    sb.append(c);
                }
            } else if (c == '\'') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '\'') {
                    ++offset;
                    sb.append('\'');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * <code>"abc"</code>
     * 
     * @param offset stmt.charAt(offset) == first <code>"</code>
     */
    private static String parseString2(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop: for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                case '0':
                    sb.append('\0');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'Z':
                    sb.append((char) 26);
                    break;
                default:
                    sb.append(c);
                }
            } else if (c == '"') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '"') {
                    ++offset;
                    sb.append('"');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * <code>AS `abc`</code>
     * 
     * @param offset stmt.charAt(offset) == first <code>`</code>
     */
    private static String parseIdentifierEscape(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop: for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '`') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '`') {
                    ++offset;
                    sb.append('`');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * @param aliasIndex for <code>AS id</code>, index of 'i'
     */
    public static String parseAlias(String stmt, final int aliasIndex) {
        if (aliasIndex < 0 || aliasIndex >= stmt.length()) {
            return null;
        }
        switch (stmt.charAt(aliasIndex)) {
        case '\'':
            return parseString(stmt, aliasIndex);
        case '"':
            return parseString2(stmt, aliasIndex);
        case '`':
            return parseIdentifierEscape(stmt, aliasIndex);
        default:
            int offset = aliasIndex;
            for (; offset < stmt.length() && CharTypes.isIdentifierChar(stmt.charAt(offset)); ++offset) {
                ;
            }
            return stmt.substring(aliasIndex, offset);
        }
    }

    /**
     * 解析注释，返回stmt中注释结尾的index
     * @param stmt
     * @param offset
     * @return
     */
    public static int comment(String stmt, int offset) {
        int len = stmt.length();
        int n = offset;
        switch (stmt.charAt(n)) {
        case '/':
            if (len > ++n && stmt.charAt(n++) == '*' && len > n + 1) {
                for (int i = n; i < len; ++i) {
                    if (stmt.charAt(i) == '*') {
                        int m = i + 1;
                        if (len > m && stmt.charAt(m) == '/') {
                            return m;
                        }
                    }
                }
            }
            break;
        case '#':
            for (int i = n + 1; i < len; ++i) {
                if (stmt.charAt(i) == '\n') {
                    return i;
                }
            }
            break;
        }
        return offset;
    }

    public static boolean currentCharIsSep(String stmt, int offset) {
        if (stmt.length() > offset) {
            switch (stmt.charAt(offset)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                return true;
            default:
                return false;
            }
        }
        return true;
    }

    /*****
     * 检查下一个字符是否为分隔符，并把偏移量加1
     */
    public static boolean nextCharIsSep(String stmt, int offset) {
        return currentCharIsSep(stmt, ++offset);
    }

    /*****
     * 检查下一个字符串是否为期望的字符串，并把偏移量移到从offset开始计算，expectValue之后的位置
     * 
     * @param stmt 被解析的sql
     * @param offset 被解析的sql的当前位置
     * @param nextExpectedString 在stmt中准备查找的字符串
     * @param checkSepChar 当找到expectValue值时，是否检查其后面字符为分隔符号
     * @return 如果包含指定的字符串，则移动相应的偏移量，否则返回值=offset
     */
    public static int nextStringIsExpectedWithIgnoreSepChar(String stmt,
                                                            int offset,
                                                            String nextExpectedString,
                                                            boolean checkSepChar) {
        if (nextExpectedString == null || nextExpectedString.length() < 1) {
            return offset;
        }
        int i = offset;
        int index = 0;
        char expectedChar;
        char actualChar;
        boolean isSep;
        for (; i < stmt.length() && index < nextExpectedString.length(); ++i) {
            if (index == 0) {
                isSep = currentCharIsSep(stmt, i);
                if (isSep) {
                    continue;
                }
            }
            actualChar = stmt.charAt(i);
            expectedChar = nextExpectedString.charAt(index++);
            if (actualChar != expectedChar) {
                return offset;
            }
        }
        if (index == nextExpectedString.length()) {
            boolean ok = true;
            if (checkSepChar) {
                ok = nextCharIsSep(stmt, i);
            }
            if (ok) {
                return i;
            }
        }
        return offset;
    }

    private static final String JSON = "json";
    private static final String EQ = "=";

    //private static final String WHERE = "where";
    //private static final String SET = "set";

    /**********
     * 检查下一个字符串是否json= *
     * 
     * @param stmt 被解析的sql
     * @param offset 被解析的sql的当前位置
     * @return 如果包含指定的字符串，则移动相应的偏移量，否则返回值=offset
     */
    public static int nextStringIsJsonEq(String stmt, int offset) {
        int i = offset;

        // / drds 之后的符号
        if (!currentCharIsSep(stmt, ++i)) {
            return offset;
        }

        // json 串
        int k = nextStringIsExpectedWithIgnoreSepChar(stmt, i, JSON, false);
        if (k <= i) {
            return offset;
        }
        i = k;

        // 等于符号
        k = nextStringIsExpectedWithIgnoreSepChar(stmt, i, EQ, false);
        if (k <= i) {
            return offset;
        }
        return i;
    }

    public static int move(String stmt, int offset, int length) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            case '/':
            case '#':
                i = comment(stmt, i);
                continue;
            default:
                return i + length;
            }
        }
        return i;
    }

    public static boolean compare(String s, int offset, char[] keyword) {
        if (s.length() >= offset + keyword.length) {
            for (int i = 0; i < keyword.length; ++i, ++offset) {
                if (Character.toUpperCase(s.charAt(offset)) != keyword[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
