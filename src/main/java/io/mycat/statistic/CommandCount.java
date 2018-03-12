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
package io.mycat.statistic;

/**
 * @author mycat
 */
public class CommandCount {

    private long initDB;
    private long query;
    private long stmtPrepare;
    private long stmtSendLongData;
    private long stmtReset;
    private long stmtExecute;
    private long stmtClose;
    private long ping;
    private long kill;
    private long quit;
    private long heartbeat;
    private long other;
    public CommandCount(){

    }
    public void doInitDB() {
        ++initDB;
    }

    public long initDBCount() {
        return initDB;
    }

    public void doQuery() {
        ++query;
    }

    public long queryCount() {
        return query;
    }

    public void doStmtPrepare() {
        ++stmtPrepare;
    }

    public long stmtPrepareCount() {
        return stmtPrepare;
    }
    
    public void doStmtSendLongData() {
    	++stmtSendLongData;
    }
    
    public long stmtSendLongDataCount() {
    	return stmtSendLongData;
    }
    
    public void doStmtReset() {
    	++stmtReset;
    }
    
    public long stmtResetCount() {
    	return stmtReset;
    }

    public void doStmtExecute() {
        ++stmtExecute;
    }

    public long stmtExecuteCount() {
        return stmtExecute;
    }

    public void doStmtClose() {
        ++stmtClose;
    }

    public long stmtCloseCount() {
        return stmtClose;
    }

    public void doPing() {
        ++ping;
    }

    public long pingCount() {
        return ping;
    }

    public void doKill() {
        ++kill;
    }

    public long killCount() {
        return kill;
    }

    public void doQuit() {
        ++quit;
    }

    public long quitCount() {
        return quit;
    }

    public void doOther() {
        ++other;
    }

    public long heartbeat() {
        return heartbeat;
    }

    public void doHeartbeat() {
        ++heartbeat;
    }

    public long otherCount() {
        return other;
    }

}