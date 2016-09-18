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
package io.mycat.server.response;


import io.mycat.MycatServer;
import io.mycat.server.ErrorCode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.loader.ReloadUtil;
import io.mycat.server.packet.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public final class RollbackCallBack {
	private static final Logger LOGGER = LoggerFactory.getLogger(RollbackCallBack.class);

	public static void execute(MySQLFrontConnection c) {
		final ReentrantLock lock = MycatServer.getInstance().getConfig().getLock();
		lock.lock();
		try {
			if (ReloadUtil.rollback()) {
				StringBuilder s = new StringBuilder();
				s.append(c).append("Rollback config success by manager");
				LOGGER.warn(s.toString());
				OkPacket ok = new OkPacket();
				ok.packetId = 1;
				ok.affectedRows = 1;
				ok.serverStatus = 2;
				ok.message = "Rollback config success".getBytes();
				ok.write(c);
			} else {
				c.writeErrMessage(ErrorCode.ER_YES, "Rollback config failure");
			}
		} finally {
			lock.unlock();
		}
	}


}