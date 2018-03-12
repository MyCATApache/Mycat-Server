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

/**
 * @author mycat
 */
public final class PairUtil {
    private static final int DEFAULT_INDEX = -1;

    /**
     * "2" -&gt; (0,2)<br/>
     * "1:2" -&gt; (1,2)<br/>
     * "1:" -&gt; (1,0)<br/>
     * "-1:" -&gt; (-1,0)<br/>
     * ":-1" -&gt; (0,-1)<br/>
     * ":" -&gt; (0,0)<br/>
     */
    public static Pair<Integer, Integer> sequenceSlicing(String slice) {
        int ind = slice.indexOf(':');
        if (ind < 0) {
            int i = Integer.parseInt(slice.trim());
            if (i >= 0) {
                return new Pair<Integer, Integer>(0, i);
            } else {
                return new Pair<Integer, Integer>(i, 0);
            }
        }
        String left = slice.substring(0, ind).trim();
        String right = slice.substring(1 + ind).trim();
        int start, end;
        if (left.length() <= 0) {
            start = 0;
        } else {
            start = Integer.parseInt(left);
        }
        if (right.length() <= 0) {
            end = 0;
        } else {
            end = Integer.parseInt(right);
        }
        return new Pair<Integer, Integer>(start, end);
    }

    /**
     * <pre>
     * 将名字和索引用进行分割 当src = "offer_group[4]", l='[', r=']'时，
     * 返回的Piar<String,Integer>("offer", 4);
     * 当src = "offer_group", l='[', r=']'时， 
     * 返回Pair<String, Integer>("offer",-1);
     * </pre>
     */
    public static Pair<String, Integer> splitIndex(String src, char l, char r) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return new Pair<String, Integer>("", DEFAULT_INDEX);
        }
        if (src.charAt(length - 1) != r) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        int offset = src.lastIndexOf(l);
        if (offset == -1) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        int index = DEFAULT_INDEX;
        try {
            index = Integer.parseInt(src.substring(offset + 1, length - 1));
        } catch (NumberFormatException e) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        return new Pair<String, Integer>(src.substring(0, offset), index);
    }

}