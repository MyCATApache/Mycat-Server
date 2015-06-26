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
package io.mycat.parser;

import io.mycat.server.parser.ServerParseSet;

/**
 * @author mycat
 */
public final class ServerParserTestPerf {

    private static void parseSetPerf() {
        // ServerParse.parse("show databases");
        // ServerParseSet.parse("set autocommit=1");
        // ServerParseSet.parse("set names=1");
        ServerParseSet.parse("SET character_set_results = NULL", 4);
        // ServerParse.parse("select id,name,value from t");
        // ServerParse.parse("select * from offer where member_id='abc'");
    }

    public static void main(String[] args) {
        parseSetPerf();
        int count = 10000000;

        System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            parseSetPerf();
        }
        long t2 = System.currentTimeMillis();

        // print time
        System.out.println("take:" + ((t2 - t1) * 1000 * 1000) / count + " ns.");
    }

}