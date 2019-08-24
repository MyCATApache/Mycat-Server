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
package io.mycat;



import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.model.SystemConfig;

/**
 * @author mycat
 */
public final class MycatStartup {
    private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatStartup.class);
    public static void main(String[] args) {
        //use zk ?
        ZkConfig.getInstance().initZk();
        try {
            String home = SystemConfig.getHomePath();
            if (home == null) {
                System.out.println(SystemConfig.SYS_HOME + "  is not set.");
                System.exit(-1);
            }
            // init
            MycatServer server = MycatServer.getInstance();
            //这个方法执行的代码，上面SystemConfig.getHomePath()已经执行过了，建议注释掉。
            //server.beforeStart();

            // startup
            server.startup();
            System.out.println("MyCAT Server startup successfully. see logs in logs/mycat.log");

        } catch (Exception e) {
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            LOGGER.error(sdf.format(new Date()) + " startup error", e);
            System.exit(-1);
        }
    }
}
