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
package org.opencloudb.heartbeat;

/**
 * @author mycat
 */
public class HeartbeatContext {

    // private final static long TIMER_PERIOD = 1000L;
    //
    // private String name;
    // private Timer timer;
    // private NIOProcessor[] processors;
    // private NIOConnector connector;
    //
    // public HeartbeatContext(String name) throws IOException {
    // this.name = name;
    // this.init();
    // }
    //
    // public void startup() {
    // // startup timer
    // timer.schedule(new TimerTask() {
    // @Override
    // public void run() {
    // TimeUtil.update();
    // }
    // }, 0L, TimeUtil.UPDATE_PERIOD);
    //
    // // startup processors
    // for (int i = 0; i < processors.length; i++) {
    // processors[i].startup();
    // }
    //
    // // startup connector
    // connector.start();
    // }
    //
    // public void doHeartbeat(HeartbeatConfig heartbeat) {
    // timer.schedule(new MySQLHeartbeatTask(connector, heartbeat), 0L,
    // TIMER_PERIOD);
    // }
    //
    // private void init() throws IOException {
    // // init timer
    // this.timer = new Timer(name + "Timer", false);
    //
    // // init processors
    // processors = new
    // NIOProcessor[Runtime.getRuntime().availableProcessors()];
    // for (int i = 0; i < processors.length; i++) {
    // processors[i] = new NIOProcessor(name + "Processor" + i);
    // }
    //
    // // init connector
    // connector = new NIOConnector(name + "Connector");
    // connector.setProcessors(processors);
    // }

}