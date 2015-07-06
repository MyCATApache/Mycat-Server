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
package io.mycat.util;

/**
 * 格式化工具
 * 
 * @author mycat
 * @version 2008-11-24 下午12:58:17
 */
public final class FormatUtil {

    // 右对齐格式化字符串
    public static final int ALIGN_RIGHT = 0;

    // 左对齐格式化字符串
    public static final int ALIGN_LEFT = 1;

    private static final char defaultSplitChar = ' ';

    private static final String[] timeFormat = new String[] { "d ", "h ", "m ", "s ", "ms" };

    /**
     * 格式化后返回的字符串
     * 
     * @param s
     *            需要格式化的原始字符串，默认按左对齐。
     * @param fillLength
     *            填充长度
     * @return String
     */
    public static String format(String s, int fillLength) {
        return format(s, fillLength, defaultSplitChar, ALIGN_LEFT);
    }

    /**
     * 格式化后返回的字符串
     * 
     * @param i
     *            需要格式化的数字类型，默认按右对齐。
     * @param fillLength
     *            填充长度
     * @return String
     */
    public static String format(int i, int fillLength) {
        return format(Integer.toString(i), fillLength, defaultSplitChar, ALIGN_RIGHT);
    }

    /**
     * 格式化后返回的字符串
     * 
     * @param l
     *            需要格式化的数字类型，默认按右对齐。
     * @param fillLength
     *            填充长度
     * @return String
     */
    public static String format(long l, int fillLength) {
        return format(Long.toString(l), fillLength, defaultSplitChar, ALIGN_RIGHT);
    }

    /**
     * @param s
     *            需要格式化的原始字符串
     * @param fillLength
     *            填充长度
     * @param fillChar
     *            填充的字符
     * @param align
     *            填充方式（左边填充还是右边填充）
     * @return String
     */
    public static String format(String s, int fillLength, char fillChar, int align) {
        if (s == null) {
            s = "";
        } else {
            s = s.trim();
        }
        int charLen = fillLength - s.length();
        if (charLen > 0) {
            char[] fills = new char[charLen];
            for (int i = 0; i < charLen; i++) {
                fills[i] = fillChar;
            }
            StringBuilder str = new StringBuilder(s);
            switch (align) {
            case ALIGN_RIGHT:
                str.insert(0, fills);
                break;
            case ALIGN_LEFT:
                str.append(fills);
                break;
            default:
                str.append(fills);
            }
            return str.toString();
        } else {
            return s;
        }
    }

    /**
     * 格式化时间输出
     * <p>
     * 1d 15h 4m 15s 987ms
     * </p>
     */
    public static String formatTime(long millis, int precision) {
        long[] la = new long[5];
        la[0] = (millis / 86400000);// days
        la[1] = (millis / 3600000) % 24;// hours
        la[2] = (millis / 60000) % 60;// minutes
        la[3] = (millis / 1000) % 60;// seconds
        la[4] = (millis % 1000);// ms

        int index = 0;
        for (int i = 0; i < la.length; i++) {
            if (la[i] != 0) {
                index = i;
                break;
            }
        }

        StringBuilder buf = new StringBuilder();
        int validLength = la.length - index;
        for (int i = 0; (i < validLength && i < precision); i++) {
            buf.append(la[index]).append(timeFormat[index]);
            index++;
        }
        return buf.toString();
    }

}