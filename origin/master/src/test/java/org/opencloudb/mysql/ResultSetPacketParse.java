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
package org.opencloudb.mysql;

import org.opencloudb.util.FormatUtil;
import org.opencloudb.util.SplitUtil;

/**
 * @author mycat
 */
public class ResultSetPacketParse {

    public static String parse(String src) {
        String[] sa = SplitUtil.split(src, ',', true);
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < sa.length;) {
            int length = Byte.parseByte(sa[i++]) & 0xff;
            length |= (Byte.parseByte(sa[i++]) & 0xff) << 8;
            length |= (Byte.parseByte(sa[i++]) & 0xff) << 16;
            s.append("Length=").append(FormatUtil.format(length, 3)).append(',');
            s.append("Id=").append(Byte.parseByte(sa[i++])).append(':');
            for (int x = 0; x < length; x++) {
                s.append(' ').append(sa[i++]);
            }
            s.append('\n');
        }
        return s.toString();
    }

    static String s = "1, 0, 0, 1, 1, 68, 0, 0, 2, 3, 100, 101, 102, 22, 99, 111, 98, 97, 114, 95, 116, 101, 115, 116, 95, 99, 111, 110, 110, 95, 98, 105, 110, 100, 95, 49, 2, 116, 49, 2, 116, 49, 10, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 10, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 5, 0, 0, 3, -2, 0, 0, 34, 0, 4, 0, 0, 4, 3, 49, 50, 51, 46, 0, 0, 5, -1, 30, 4, 85, 110, 107, 110, 111, 119, 110, 32, 99, 111, 108, 117, 109, 110, 32, 39, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 39, 32, 105, 110, 32, 39, 102, 105, 101, 108, 100, 32, 108, 105, 115, 116, 39";
    static String s2 = "1, 0, 0, 1, 1, 68, 0, 0, 2, 3, 100, 101, 102, 22, 99, 111, 98, 97, 114, 95, 116, 101, 115, 116, 95, 99, 111, 110, 110, 95, 98, 105, 110, 100, 95, 49, 2, 116, 49, 2, 116, 49, 10, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 10, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 5, 0, 0, 3, -2, 0, 0, 34, 0, 4, 0, 0, 4, 3, 49, 50, 51, 4, 0, 0, 5, 3, 49, 50, 51, 46, 0, 0, 6, -1, 30, 4, 85, 110, 107, 110, 111, 119, 110, 32, 99, 111, 108, 117, 109, 110, 32, 39, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 39, 32, 105, 110, 32, 39, 102, 105, 101, 108, 100, 32, 108, 105, 115, 116, 39";
    static String s3 = "1, 0, 0, 1, 1, 46, 0, 0, 1, -1, 30, 4, 85, 110, 107, 110, 111, 119, 110, 32, 99, 111, 108, 117, 109, 110, 32, 39, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 39, 32, 105, 110, 32, 39, 102, 105, 101, 108, 100, 32, 108, 105, 115, 116, 39";
    static String s4 = "1, 0, 0, 1, 1, 68, 0, 0, 2, 3, 100, 101, 102, 22, 99, 111, 98, 97, 114, 95, 116, 101, 115, 116, 95, 99, 111, 110, 110, 95, 98, 105, 110, 100, 95, 49, 2, 116, 49, 2, 116, 49, 10, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 10, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 5, 0, 0, 3, -2, 0, 0, 34, 0, 4, 0, 0, 4, 3, 49, 50, 51, 4, 0, 0, 5, 3, 49, 50, 51, 46, 0, 0, 6, -1, 30, 4, 85, 110, 107, 110, 111, 119, 110, 32, 99, 111, 108, 117, 109, 110, 32, 39, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 39, 32, 105, 110, 32, 39, 102, 105, 101, 108, 100, 32, 108, 105, 115, 116, 39";
    static String s5 = "1, 0, 0, 1, 1, 1, 0, 0, 2, 1, 46, 0, 0, 3, -1, 30, 4, 85, 110, 107, 110, 111, 119, 110, 32, 99, 111, 108, 117, 109, 110, 32, 39, 114, 101, 97, 100, 69, 114, 114, 67, 111, 108, 39, 32, 105, 110, 32, 39, 102, 105, 101, 108, 100, 32, 108, 105, 115, 116, 39";

    public static void main(String[] args) {
        System.out.println(ResultSetPacketParse.parse(s));
        System.out.println(ResultSetPacketParse.parse(s2));
        System.out.println(ResultSetPacketParse.parse(s3));
        System.out.println(ResultSetPacketParse.parse(s4));
        System.out.println(ResultSetPacketParse.parse(s5));
    }

}