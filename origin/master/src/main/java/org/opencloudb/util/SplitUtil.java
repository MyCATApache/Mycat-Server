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
package org.opencloudb.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author mycat
 */
public class SplitUtil {
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * 解析字符串<br>
     * 比如:c1='$',c2='-' 输入字符串：mysql_db$0-2<br>
     * 输出array:mysql_db[0],mysql_db[1],mysql_db[2]
     */
    public static String[] split2(String src, char c1, char c2) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<String>();
        String[] p = split(src, c1, true);
        if (p.length > 1) {
            String[] scope = split(p[1], c2, true);
            int min = Integer.parseInt(scope[0]);
            int max = Integer.parseInt(scope[scope.length - 1]);
            for (int x = min; x <= max; x++) {
                list.add(new StringBuilder(p[0]).append('[').append(x).append(']').toString());
            }
        } else {
            list.add(p[0]);
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src) {
        return split(src, null, -1);
    }

    public static String[] split(String src, char separatorChar) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<String>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < length) {
            if (src.charAt(i) == separatorChar) {
                if (match) {
                    list.add(src.substring(start, i));
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            list.add(src.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src, char separatorChar, boolean trim) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<String>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < length) {
            if (src.charAt(i) == separatorChar) {
                if (match) {
                    if (trim) {
                        list.add(src.substring(start, i).trim());
                    } else {
                        list.add(src.substring(start, i));
                    }
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            if (trim) {
                list.add(src.substring(start, i).trim());
            } else {
                list.add(src.substring(start, i));
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String str, String separatorChars) {
        return split(str, separatorChars, -1);
    }

    public static String[] split(String src, String separatorChars, int max) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<String>();
        int sizePlus1 = 1;
        int i = 0;
        int start = 0;
        boolean match = false;
        if (separatorChars == null) {// null表示使用空白作为分隔符
            while (i < length) {
                if (Character.isWhitespace(src.charAt(i))) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {// 优化分隔符长度为1的情形
            char sep = separatorChars.charAt(0);
            while (i < length) {
                if (src.charAt(i) == sep) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        } else {// 一般情形
            while (i < length) {
                if (separatorChars.indexOf(src.charAt(i)) >= 0) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        }
        if (match) {
            list.add(src.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * 解析字符串，比如: <br>
     * 1. c1='$',c2='-',c3='[',c4=']' 输入字符串：mysql_db$0-2<br>
     * 输出mysql_db[0],mysql_db[1],mysql_db[2]<br>
     * 2. c1='$',c2='-',c3='#',c4='0' 输入字符串：mysql_db$0-2<br>
     * 输出mysql_db#0,mysql_db#1,mysql_db#2<br>
     * 3. c1='$',c2='-',c3='0',c4='0' 输入字符串：mysql_db$0-2<br>
     * 输出mysql_db0,mysql_db1,mysql_db2<br>
     */
    public static String[] split(String src, char c1, char c2, char c3, char c4) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new LinkedList<String>();
        if (src.indexOf(c1) == -1) {
            list.add(src.trim());
        } else {
            String[] s = split(src, c1, true);
            String[] scope = split(s[1], c2, true);
            int min = Integer.parseInt(scope[0]);
            int max = Integer.parseInt(scope[scope.length - 1]);
            if (c3 == '0') {
                for (int x = min; x <= max; x++) {
                    list.add(new StringBuilder(s[0]).append(x).toString());
                }
            } else if (c4 == '0') {
                for (int x = min; x <= max; x++) {
                    list.add(new StringBuilder(s[0]).append(c3).append(x).toString());
                }
            } else {
                for (int x = min; x <= max; x++) {
                    list.add(new StringBuilder(s[0]).append(c3).append(x).append(c4).toString());
                }
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src, char fi, char se, char th) {
        return split(src, fi, se, th, '0', '0');
    }

    public static String[] split(String src, char fi, char se, char th, char left, char right) {
        List<String> list = new LinkedList<String>();
        String[] pools = split(src, fi, true);
        for (int i = 0; i < pools.length; i++) {
            if (pools[i].indexOf(se) == -1) {
                list.add(pools[i]);
                continue;
            }
            String[] s = split(pools[i], se, th, left, right);
            for (int j = 0; j < s.length; j++) {
                list.add(s[j]);
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] splitByByteSize(String string, int size) {
        if (size < 2)
        {
         return    new String[]{string};
        }
        byte[] bytes = string.getBytes();
        if (bytes.length <= size)
            return new String[] { string };
        // 分成的条数不确定(整除的情况下也许会多出一条),所以先用list再转化为array
        List list = new ArrayList();
        int offset = 0;// 偏移量,也就是截取的字符串的首字节的位置
        int length = 0;// 截取的字符串的长度,可能是size,可能是size-1
        int position = 0;// 可能的截取点,根据具体情况判断是不是在此截取
        while (position < bytes.length) {
            position = offset + size;
            if (position > bytes.length) {
                // 最后一条
                String s = new String(bytes, offset, bytes.length - offset);
                list.add(s);
                break;
            }
            if (bytes[position - 1] > 0
                    || (bytes[position - 1] < 0 && bytes[position - 2] < 0))
                // 截断点是字母,或者是汉字
                length = size;
            else
                // 截断点在汉字中间
                length = size - 1;
            String s = new String(bytes, offset, length);
            list.add(s);
            offset += length;
        }
        String[] array = new String[list.size()];
        for (int i = 0; i < array.length; i++)
            array[i] = (String) list.get(i);
        return array;
    }

}